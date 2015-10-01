##' Create a task handle object.  This is a "pointer" to a task and
##' can be used to retrieve information about status, running times,
##' expression and the result of the task once complete.  Generally
##' you do not need to make a task object as they will be created for
##' you by things like the \code{task_get} method of the
##' \code{\link{observer}} and \code{\link{queue}} objects.
##'
##' Tasks have a \emph{unique identifier}; these are unique within a
##' queue and are implemented as an incrementing integer.  However,
##' this is an implementation detail and should not be relied on.  The
##' identifier is represented as a \emph{character string} rather than
##' an integer in most places.
##'
##' Tasks exist in one of a number of \emph{statuses}.  See the
##' \code{status} method below for a list of possible statuses and
##' their interpretation.
##'
##' @template task_methods
##' @title Create a task handle
##' @param obj A \code{queue} or \code{observer} object.
##' @param task_id Task identifier
##' @param key_complete If known, specify the \code{key_complete},
##' otherwise we look it up on creation.
##' @export
task <- function(obj, task_id, key_complete=NULL) {
  .R6_task$new(obj, task_id, key_complete)
}

## First, the ideal lifecycle:
## * after submissing a job is pending (time_sub)
TASK_PENDING  <- "PENDING"
## * after it is picked up by a worker it is running (time_beg)
TASK_RUNNING  <- "RUNNING"
## * after it is finished by a worker it is complete or error (time_end)
TASK_COMPLETE <- "COMPLETE"
TASK_ERROR    <- "ERROR"

## Alternatively:
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

    result=function(follow_redirect=FALSE) {
      task_result(self$con, self$keys, self$id, follow_redirect)
    },

    expr=function(locals=FALSE) {
      task_expr(self$con, self$keys, self$id,
                if (locals) object_cache(self$keys$objects, self$con))
    },

    envir=function() {
      unname(tasks_envir(self$con, self$keys, self$id))
    },

    times=function(unit_elapsed="secs") {
      tasks_times(self$con, self$keys, self$id, unit_elapsed)
    },

    wait=function(timeout, every=0.05) {
      task_wait(self$con, self$keys, self$id, timeout, every)
    }
  ))

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
  as.character(con$HKEYS(keys$tasks_status))
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

tasks_overview <- function(con, keys, task_ids=NULL) {
  lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
  status <- tasks_status(con, keys, task_ids)
  lvls <- c(lvls, setdiff(unique(status), lvls))
  table(factor(status, lvls))
}

tasks_envir <- function(con, keys, task_ids=NULL) {
  from_redis_hash(con, keys$tasks_envir, task_ids)
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
  ret <- data.frame(task_id   = task_ids,
                    submitted = f(keys$tasks_time_sub),
                    started   = f(keys$tasks_time_beg),
                    finished  = f(keys$tasks_time_end),
                    stringsAsFactors=FALSE)
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
tasks_groups_list <- function(con, keys) {
  unique(as.character(con$HVALS(keys$tasks_group)))
}

## Then, the tasks associated with a given group:
tasks_in_groups <- function(con, keys, groups) {
  ## TODO: This should be done with HSCAN do do it "properly", but
  ## that should probably move into the RedisAPI before I try.
  groups_hash <- from_redis_hash(con, keys$tasks_group)
  names(groups_hash)[groups_hash %in% groups]
}

## TODO: not tested anywhere yet.
tasks_lookup_group <- function(con, keys, task_ids=NULL) {
  if (is.null(task_ids)) {
    task_ids <- tasks_list(con, keys)
  }
  ## TODO: this is not how we usually do this...
  ## TODO: there are alot of possible edge cases here the should be
  ## tested, especially missing values
  groups <- from_redis_hash(con, keys$tasks_group)
  setNames(groups[task_ids], task_ids)
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

task_wait <- function(con, keys, task_id, timeout, every=0.05) {
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")
  repeat {
    res <- task_result(con, keys, task_id, sanitise=TRUE)
    if (!inherits(res, "UnfetchableTask")) {
      return(res)
    } else if (Sys.time() - t0 < timeout) {
      Sys.sleep(every)
    } else {
      stop("task not returned in time")
    }
  }
}
