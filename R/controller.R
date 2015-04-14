## TODO: queue objects should be able to be destroyed at will: all the
## data should be stored on the server; requires reconfiguring the
## initialize method though...

## TODO: Refuse to run any more jobs unless the environment agrees;
## that requires rehashing and is potentially expensive.

.R6_queue <- R6::R6Class(
  "queue",

  public=list(
    con=NULL,
    name=NULL,
    keys=NULL,
    objects=NULL,
    envir=NULL,
    envir_id=NULL,

    initialize=function(name, packages=NULL, sources=NULL, con=NULL,
                        clean=FALSE) {
      self$con <- redis_connection(con)
      self$name <- name
      self$keys <- rrqueue_keys(self$name)

      existing <- self$con$SISMEMBER(self$keys$rrqueue_queues, self$name)
      if (existing) {
        if (clean) {
          message("cleaning old queue")
          self$clean()
        } else {
          message("reattaching to existing queue")
        }
      } else {
        message("creating new queue")
        self$clean()
      }
      nw <- self$n_workers()
      if (nw > 0L) {
        message(sprintf("%d workers available", nw))
      }

      self$con$SADD(self$keys$rrqueue_queues, self$name)
      self$initialize_environment(packages, sources, TRUE)
      self$objects <- object_cache(con, self$keys$objects)
    },

    clean=function() {
      self$con$SREM(self$keys$rrqueue_queues, self$name)

      ## TODO: This one here seems daft.  If there are workers they
      ## might still be around, and they might be working on tasks.
      ## Might be best not to get too involved with modifying the
      ## worker queue, aside from messaging, really; leave deleting
      ## worker queues to a standalone function?
      ##
      ##   self$con$DEL(self$keys$workers_name)
      ##   self$con$DEL(self$keys$workers_status)
      ##   self$con$DEL(self$keys$workers_task)

      self$con$DEL(self$keys$tasks_counter)
      self$con$DEL(self$keys$tasks_id)
      self$con$DEL(self$keys$tasks_expr)
      self$con$DEL(self$keys$tasks_status)
      self$con$DEL(self$keys$tasks_result)
      self$con$DEL(self$keys$tasks_envir)

      self$con$DEL(self$keys$envirs_contents)
    },

    ## TODO: facility for named environnents?
    ## TODO: facility for deleting environments?
    initialize_environment=function(packages, sources, set_default=FALSE) {
      if (!is.null(self$envir)) {
        stop("objects environments are immutable(-ish)")
      }
      ## First, we need to load this environment ourselves.
      envir <- new.env(parent=baseenv())
      source_files <- create_environment2(packages, sources, envir)

      dat <- list(packages=packages,
                  sources=sources,
                  source_files=source_files)

      dat_str <- object_to_string(dat)
      self$envir <- envir
      self$envir_id <- hash_string(dat_str)
      self$con$HSET(self$keys$envirs_contents, self$envir_id, dat_str)
    },

    ## TODO: clean up queues on startup, or attach to existing queue?
    ## TODO: spin up workers?
    ## TODO: pending, completed, etc.
    ## TODO: allow setting a "group" or "name" for more easily
    ## recalling jobs?
    ## TODO: envir should be parent.frame?
    enqueue=function(expr, envir=.GlobalEnv, key_complete=NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },

    enqueue_=function(expr, envir=.GlobalEnv, key_complete=NULL) {
      dat <- prepare_expression(expr)
      task_id <- as.character(self$con$INCR(self$keys$tasks_counter))
      expr_str <- save_expression(dat, task_id, envir, self$objects)

      if (is.null(key_complete)) {
        key_complete <- rrqueue_key_task_complete(self$name, task_id)
      }
      redis_multi(self$con, {
        self$con$HSET(self$keys$tasks_expr,     task_id, expr_str)
        self$con$HSET(self$keys$tasks_envir,    task_id, self$envir_id)
        self$con$HSET(self$keys$tasks_complete, task_id, key_complete)
        self$con$HSET(self$keys$tasks_status,   task_id, TASK_PENDING)
        self$con$RPUSH(self$keys$tasks_id,      task_id)
      })
      invisible(task(self$con, self$name, task_id, key_complete))
    },

    requeue=function(task_id) {
      con <- self$con
      keys <- self$keys

      status <- con$HGET(keys$tasks_status, task_id)
      if (status != TASK_ORPHAN) {
        stop("Can only reqeueue orphaned tasks")
      }

      ## TODO: The migration could happen in a lua script.
      expr_str     <- con$HGET(keys$tasks_expr,     task_id)
      envir_id     <- con$HGET(keys$tasks_envir,    task_id)
      key_complete <- con$HGET(keys$tasks_complete, task_id)

      task2_id <- as.character(con$INCR(keys$tasks_counter))

      key_complete_orphan <- paste0(key_complete, ":orphan")

      redis_multi(con, {
        ## information about the old, abandoned job:
        con$HSET(keys$tasks_complete, task_id, key_complete_orphan)
        con$HSET(keys$tasks_redirect, task_id, task2_id)
        con$HSET(keys$tasks_status,   task_id, TASK_REDIRECT)
        ## information for the new job
        con$HSET(keys$tasks_expr,     task2_id, expr_str)
        con$HSET(keys$tasks_envir,    task2_id, envir_id)
        con$HSET(keys$tasks_complete, task2_id, key_complete)
        con$HSET(keys$tasks_status,   task2_id, TASK_PENDING)
        con$RPUSH(keys$tasks_id,      task2_id)
      })
      task(con, self$name, task2_id, key_complete)
    },

    task=function(task_id) {
      key_complete <- self$con$HGET(self$keys$tasks_complete, task_id)
      task(self$con, self$name, task_id, key_complete)
    },

    ## TODO: Send messages to workers.  This can be a second list and
    ## may have broadcast and specific messages.  We could use that to
    ## advertise that we're not interested in new jobs.
    ## Alternatively, we could just have this for workers that we spin
    ## up ourselves.

    workers=function() {
      as.character(self$con$SMEMBERS(self$keys$workers_name))
    },

    ## These messages are *broadcast* commands.  No data will be
    ## returned by the worker.
    send_message=function(content, worker=NULL) {
      if (is.null(worker)) {
        worker <- self$workers()
      }
      ## TODO: check if the worker exists before pushing anything onto
      ## its message queue.
      key <- rrqueue_key_worker_message(self$name, worker)
      for (k in key) {
        self$con$RPUSH(k, content)
      }
    },

    ## Semantics here are going to be really nasty if there are
    ## pending messages; it might make sense to block for m
    fetch_message=function(worker, timeout=5) {
      if (timeout <= 0) {
        stop("Infinite timeout is not clever here")
      }
      key <- key_worker(self$name, worker)$worker_response

    },

    n_workers=function() {
      ## NOTE: this is going to be an *estimate* because there might
      ## be old workers floating around.
      ##
      ## TODO: Drop orphan workers here.
      self$con$SCARD(self$keys$workers_name)
    },

    workers_status=function() {
      from_redis_hash(self$con, self$keys$workers_status)
    },

    tasks=function() {
      ## TODO: this is no longer useful, really.
      from_redis_hash(self$con, self$keys$tasks_expr)
    },

    tasks_status=function(id=NULL) {
      if (is.null(id)) {
        from_redis_hash(self$con, self$keys$tasks_status,
                        as.character)
      } else {
        status <- self$con$HMGET(self$keys$tasks_status, id)
        status[vapply(status, is.null, logical(1))] <- TASK_MISSING
        status <- as.character(status)
        names(status) <- id
        status
      }
    },

    ## TODO: Only works for one task - but name suggests >= 1
    ## TODO: write vectorised version that always returns a list
    tasks_collect=function(id) {
      status <- self$tasks_status(id)
      if (status == TASK_MISSING) {
        stop("task does not exist")
      } else if (status != TASK_COMPLETE) {
        stop("task is incomplete")
      }
      string_to_object(self$con$HGET(self$keys$tasks_result, id))
    },

    tasks_drop=function(id) {
      con <- self$con
      keys <- self$keys

      status <- self$tasks_status(id)

      if (any(status == TASK_RUNNING)) {
        stop("One of the tasks is running -- not clear how to deal")
      }

      ret <- logical(length(id))
      names(ret) <- id

      redis_multi(con, {
        for (i in id[status == TASK_PENDING]) {
          ret[[i]] <- self$con$LREM(keys$tasks_id, 0, i) > 0L
        }
        con$HDEL(keys$tasks_expr,     id)
        con$HDEL(keys$tasks_status,   id)
        con$HDEL(keys$tasks_envir,    id)
        con$HDEL(keys$tasks_complete, id)
        con$HDEL(keys$tasks_result,   id)
      })

      ret
    }
  ))

##' Create an rrqueue queue
##' @title Create an rrqueue queue
##' @param name Queue name
##' @param packages Character vector of packages to load
##' @param sources Character vector of files to source
##' @param con Connection to a redis database
##' @param clean Delete any rements of existing queues on startup
##' (this can cause things to go haywire if processes are still live
##' working on jobs as they'll clobber your queue).
##' @export
queue <- function(name, packages=NULL, sources=NULL, con=NULL, clean=FALSE) {
  .R6_queue$new(name, packages, sources, con)
}

queues <- function(con=NULL) {
  con <- redis_connection(con)
  as.character(con$SMEMBERS("rrqueue:queues"))
}
