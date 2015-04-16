## TODO: queue objects should be able to be destroyed at will: all the
## data should be stored on the server; requires reconfiguring the
## initialize method though...

## TODO: Refuse to run any more jobs unless the environment agrees;
## that requires rehashing and is potentially expensive.
.R6_queue <- R6::R6Class(
  "queue",

  public=list(
    con=NULL,
    queue_name=NULL,
    keys=NULL,
    objects=NULL,
    envir=NULL,
    envir_id=NULL,

    initialize=function(queue_name, packages, sources, redis_host, redis_port) {
      self$con  <- redis_connection(redis_host, redis_port)
      self$queue_name <- queue_name
      self$keys <- rrqueue_keys(self$queue_name)

      existing <- self$con$SISMEMBER(self$keys$rrqueue_queues, self$queue_name)
      if (existing) {
        message("reattaching to existing queue")
      } else {
        message("creating new queue")
        self$clean()
      }
      ## NOTE: this is not very accurate...
      nw <- self$n_workers()
      if (nw > 0L) {
        message(sprintf("%d workers available", nw))
      }

      self$con$SADD(self$keys$rrqueue_queues, self$queue_name)
      self$initialize_environment(packages, sources, TRUE)
      self$objects <- object_cache(self$con, self$keys$objects)
    },

    clean=function() {
      self$con$SREM(self$keys$rrqueue_queues, self$queue_name)

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
      self$con$DEL(self$keys$tasks_time_sub)
      self$con$DEL(self$keys$tasks_time_beg)
      self$con$DEL(self$keys$tasks_time_end)

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
        key_complete <- rrqueue_key_task_complete(self$queue_name, task_id)
      }
      time <- redis_time(self$con)
      redis_multi(self$con, {
        self$con$HSET(self$keys$tasks_expr,     task_id, expr_str)
        self$con$HSET(self$keys$tasks_envir,    task_id, self$envir_id)
        self$con$HSET(self$keys$tasks_complete, task_id, key_complete)
        self$con$HSET(self$keys$tasks_status,   task_id, TASK_PENDING)
        self$con$HSET(self$keys$tasks_time_sub, task_id, time)
        self$con$RPUSH(self$keys$tasks_id,      task_id)
      })
      invisible(task(self$con, self$queue_name, task_id, key_complete))
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
      time <- redis_time(con)
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
        con$HSET(keys$tasks_time_sub, task2_id, time)
        con$RPUSH(keys$tasks_id,      task2_id)
      })
      task(con, self$queue_name, task2_id, key_complete)
    },

    task=function(task_id) {
      key_complete <- self$con$HGET(self$keys$tasks_complete, task_id)
      task(self$con, self$queue_name, task_id, key_complete)
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
      key <- rrqueue_key_worker_message(self$queue_name, worker)
      for (k in key) {
        self$con$RPUSH(k, content)
      }
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

    tasks_status=function(task_ids=NULL) {
      from_redis_hash(self$con, self$keys$tasks_status, task_ids,
                      as.character, TASK_MISSING)
    },

    ## TODO: Only works for one task - but name suggests >= 1
    ## TODO: write vectorised version that always returns a list
    ## TODO: worth pushing the object through our cache or something?
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
##' @param queue_name Queue name
##' @param packages Character vector of packages to load
##' @param sources Character vector of files to source
##' @param redis_host Redis hostname
##' @param redis_port Redis port number
##' @param clean Delete any rements of existing queues on startup
##' (this can cause things to go haywire if processes are still live
##' working on jobs as they'll clobber your queue).
##' @export
queue <- function(queue_name, packages=NULL, sources=NULL,
                  redis_host="127.0.0.1", redis_port=6379,
                  clean=FALSE) {
  .R6_queue$new(queue_name, packages, sources, redis_host, redis_port)
}
