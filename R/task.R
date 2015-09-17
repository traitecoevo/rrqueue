## First, the ideal lifecycle:
## * after submissing a job is pending (time_sub)
TASK_PENDING  <- "PENDING"
## * after it is picked up by a worker it is running (time_beg)
TASK_RUNNING  <- "RUNNING"
## * after it is finished by a worker it is complete or error (time_end)
TASK_COMPLETE <- "COMPLETE"
TASK_ERROR    <- "ERROR"

## Alternatively:
## the environment failed to work
TASK_ENVIR_ERROR <- "ENVIR_ERROR"
## worker node died
TASK_ORPHAN   <- "ORPHAN"
## orphaned task was requeued
TASK_REDIRECT <- "REDIRECT"
## An unknown task
TASK_MISSING  <- "MISSING"

.R6_task <- R6::R6Class(
  "task",

  public=list(
    con=NULL,
    id=NULL,
    keys=NULL,
    key_complete=NULL,

    initialize=function(obj, id, key_complete=NULL) {
      self$con <- obj$con
      self$keys <- obj$keys

      self$id  <- as.character(id)
      if (is.null(key_complete)) {
        key_complete <- self$con$HGET(self$keys$tasks_complete, id)
      }
      self$key_complete <- key_complete
    },

    ## TODO: new methods:
    ##   - drop / cancel / kill [hmm]

    ## TODO: These could be active bindings, but that might require a
    ## new R6 to CRAN to work nicely.
    status=function(follow_redirect=FALSE) {
      unname(tasks_status(self$con, self$keys, self$id, follow_redirect))
    },

    ## NOTE: sanitise is used here for the case where we want to
    ## collect results from a lot of tasks, some might have been
    ## dropped, but we don't want things to stop.  It's used in the
    ## rrqlapply and enqueue_bulk interfaces.
    result=function(follow_redirect=FALSE, sanitise=FALSE) {
      task_result(self$con, self$keys, self$id, follow_redirect, sanitise)
    },

    ## TODO: Better handling of locals?
    expr=function() {
      task_expr(self$con, self$keys, self$id)
    },

    envir=function() {
      tasks_envir(self$con, self$keys, self$id)
    },

    times=function() {
      tasks_times(self$con, self$keys, self$id)
    }
  ))

task <- function(obj, id, key_complete=NULL) {
  .R6_task$new(obj, id, key_complete)
}

## TODO: This is going to hit status too many times.  Don't worry
## about this for now, but if speed becomes important this is a
## reasonable place to look.
##
## TODO: Scripting this is a little tricky because of the redirect
## loop; do do that in Lua requires implementing most of the work in
## tasks_status in Lua; that's going to be more work than is worth it
## right now, IMO.
task_result <- function(con, keys, task_id,
                        follow_redirect=FALSE, sanitise=FALSE) {
  status <- tasks_status(con, keys, task_id, follow_redirect=FALSE)

  if (follow_redirect && status == TASK_REDIRECT) {
    task_id <- task_redirect_target(con, keys, task_id)
    status <- tasks_status(con, keys, task_id, follow_redirect=FALSE)
  }

  if (status == TASK_COMPLETE || status == TASK_ERROR) {
    string_to_object(con$HGET(keys$tasks_result, task_id))
  } else if (sanitise) {
    UnfetchableTask(task_id, status)
  } else {
    stop(sprintf("task %s is unfetchable: %s", task_id, status))
  }
}

task_redirect_target <- function(con, keys, task_id) {
  to <- con$HGET(keys$tasks_redirect, task_id)
  if (is.null(to)) {
    task_id
  } else {
    task_redirect_target(con, keys, to)
  }
}

task_expr <- function(con, keys, task_id, object_cache=NULL) {
  task_expr <- con$HGET(keys$tasks_expr, task_id)
  if (!is.null(object_cache)) {
    e <- new.env(parent=baseenv())
    expr <- restore_expression(task_expr, e, object_cache)
    attr(expr, "envir") <- e
    expr
  } else {
    restore_expression(task_expr, NULL, NULL)
  }
}

tasks_list <- function(con, keys) {
  ## NOTE: it might be useful to have pending tasks function, too;
  ## that'd look like
  ##   as.character(con$LRANGE(keys$tasks_id, 0, -1))
  ## with a len function of
  ##   con$LLEN(keys$tasks_id)
  as.character(con$HKEYS(keys$tasks_status))
}

tasks_len <- function(con, keys) {
  con$HLEN(keys$tasks_status)
}

tasks_status <- function(con, keys, task_ids=NULL, follow_redirect=FALSE) {
  ret <- from_redis_hash(con, keys$tasks_status, task_ids,
                         as.character, TASK_MISSING)
  if (follow_redirect) {
    task_ids <- names(ret)
    i <- ret == TASK_REDIRECT
    if (any(i)) {
      task2_id <- vcapply(task_ids[i],
                          function(t) task_redirect_target(con, keys, t))
      ret[i] <- unname(tasks_status(con, keys, task2_id, FALSE))
    }
  }
  ret
}

tasks_overview <- function(con, keys) {
  lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
  status <- tasks_status(con, keys)
  lvls <- c(lvls, setdiff(unique(status), lvls))
  table(factor(status, lvls))
}

tasks_envir <- function(con, keys, task_ids=NULL) {
  from_redis_hash(con, keys$tasks_envir, task_ids)
}

tasks_expr <- function(con, keys, task_ids, ...) {
  setNames(lapply(task_ids, function(i) task_expr(con, keys, i, ...)),
           task_ids)
}

tasks_times <- function(con, keys, task_ids=NULL, unit_elapsed="secs") {
  ## TODO: This could make a lot of requests; might be worth
  ## thinking about a little...
  f <- function(key) {
    from_redis_hash(con, key, task_ids, redis_time_to_r)
  }
  if (is.null(task_ids)) {
    task_ids <- tasks_list(con, keys)
  }
  ret <- data.frame(submitted = f(keys$tasks_time_sub),
                    started   = f(keys$tasks_time_beg),
                    finished  = f(keys$tasks_time_end))
  now <- redis_time_to_r(redis_time(con))
  started2  <- ret$started
  finished2 <- ret$finished
  finished2[is.na(finished2)] <- started2[is.na(started2)] <- now
  ret$waiting <- as.numeric(started2  - ret$submitted, unit_elapsed)
  ret$running <- as.numeric(finished2 - ret$started,   unit_elapsed)
  ret$idle    <- as.numeric(now       - ret$finished,  unit_elapsed)
  ret
}

## Lookup functions.
## First, find the names of extant groups:
tasks_groups <- function(con, keys) {
  unique(as.character(con$HVALS(keys$tasks_group)))
}

## Then, the tasks associated with a given group:
tasks_in_groups <- function(con, keys, groups) {
  ## TODO: This should be done with HSCAN do do it "properly", but
  ## that should probably move into the RedisAPI before I try.
  groups_hash <- from_redis_hash(con, keys$tasks_group)
  names(groups_hash)[groups_hash %in% groups]
}

tasks_lookup_group <- function(con, keys, task_ids) {
  groups <- from_redis_hash(con, keys$tasks_group)
  ret <- groups[tasks_ids]
  names(ret) <- tasks_ids
  ret
}

tasks_set_group <- function(con, keys, task_ids, group,
                            exists_action="stop") {
  ## Alternatively we could recycle?
  if (!is.null(group)) {
    assert_scalar_character(group)
  }
  exists_action <- match_value(exists_action,
                               c("stop", "warn", "pass", "overwrite"))
  ## should check that the ids are valid I think.
  ## This is pretty nasty:
  if (!is.null(group) && exists_action != "overwrite") {
    cur <- from_redis_hash(con, keys$tasks_group, task_ids)
    ok <- is.na(cur) | cur == group
    if (!all(ok)) {
      if (exists_action != "pass") {
        msg <- paste0("Groups already exist for tasks: ",
                      paste(task_ids[!ok], collapse=", "))
        if (exists_action == "stop") {
          stop(msg)
        } else {
          warning(msg, immediate.=TRUE)
        }
      }
      task_ids <- task_ids[ok]
    }
  }
  if (is.null(group)) {
    con$HDEL(keys$tasks_group, task_ids)
  } else {
    con$HMSET(keys$tasks_group, task_ids, group)
  }
  invisible(NULL)
}
