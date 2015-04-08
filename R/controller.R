object_to_string <- rrlite::object_to_string
string_to_object <- rrlite::string_to_object

TASK_PENDING <- 0L
TASK_RUNNING <- 1L
TASK_COMPLETE <- 2L

## TODO: queue objects should be able to be destroyed at will: all the
## data should be stored on the server; requires reconfiguring the
## initialize method though...

.R6_queue <- R6::R6Class(
  "queue",

  public=list(
    con=NULL,
    name=NULL,
    keys=NULL,
    objects=NULL,

    initialize=function(name, packages=NULL, sources=NULL, con=NULL) {
      self$con <- redis_connection(con)
      self$name <- name
      self$keys <- rrqueue_keys(name)
      self$objects <- object_cache(con, self$keys$objects)
      self$con$SET(self$keys$packages, object_to_string(packages))
      self$con$SET(self$keys$sources,  object_to_string(sources))

      ## This is dangerous because it will delete things in a running
      ## queue if a second queue object is created!
      self$con$DEL(self$keys$tasks)
      self$con$DEL(self$keys$tasks_counter)
      self$con$DEL(self$keys$tasks_id)
      self$con$DEL(self$keys$tasks_status)
      self$con$DEL(self$keys$tasks_result)

      ## delete workers and workers_status
      self$con$DEL(self$keys$workers)
      self$con$DEL(self$keys$workers_status)
    },

    ## TODO: clean up queues on startup, or attach to existing queue?
    ## TODO: spin up workers?
    ## TODO: pending, completed, etc.
    ## TODO: allow setting a "group" or "name" for more easily
    ## recalling jobs?
    ## TODO: should be parent.frame?
    enqueue=function(expr, envir=.GlobalEnv) {
      con <- self$con
      keys <- self$keys

      task_id <- con$INCR(keys$tasks_counter)
      prefix <- paste0(".", task_id, ":")

      expr <- substitute(expr)
      expr <- save_expression(expr, prefix, envir, self$objects)

      con$HSET(keys$tasks, task_id, expr$str)
      con$HSET(keys$tasks_status, task_id, TASK_PENDING)
      con$RPUSH(keys$tasks_id, task_id)

      if (length(expr$objects) > 0L) {
        con$LPUSH(rrqueue_key_task_objects(self$name, task_id),
                  expr$objects)
      }

      ## NOTE: coercing this to a string because that's mostly how
      ## tasks will be done.
      as.character(task_id)
    },

    ## TODO: Send messages to workers.  This can be a second list and
    ## may have broadcast and specific messages.  We could use that to
    ## advertise that we're not interested in new jobs.
    ## Alternatively, we could just have this for workers that we spin
    ## up ourselves.

    workers=function() {
      as.character(self$con$SMEMBERS(self$keys$workers))
    },

    ## These messages are *broadcast* commands.  No data will be
    ## returned by the worker.
    send_message=function(content, worker=NULL) {
      if (is.null(worker)) {
        worker <- self$workers()
      }
      ## TODO: check if the worker exists before pushing anything onto
      ## its message queue.
      key <- rrqueue_key_worker(self$name, worker)
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
      self$con$SCARD(self$keys$workers)
    },

    workers_status=function() {
      from_redis_hash(self$con, self$keys$workers_status)
    },

    tasks=function() {
      from_redis_hash(self$con, self$keys$tasks)
    },

    tasks_status=function() {
      from_redis_hash(self$con, self$keys$tasks_status, as.integer)
    },

    tasks_collect=function(id) {
      status <- as.integer(self$con$HGET(self$keys$tasks_status, id))
      if (status != TASK_COMPLETE) {
        stop("task is incomplete")
      }
      string_to_object(self$con$HGET(self$keys$tasks_result, id))
    },

    tasks_drop=function(id) {
      con <- self$con
      keys <- self$keys

      status <- con$HMGET(keys$tasks_status, id)
      status[vapply(status, is.null, logical(1))] <- FALSE
      status <- as.integer(status)
      if (any(status == TASK_RUNNING)) {
        stop("One of the tasks is running -- not clear how to deal")
      }

      ret <- redis_multi(con, {
        for (i in id[status == TASK_PENDING]) {
          self$con$LREM(keys$tasks_id, 0, i)
        }
        con$HDEL(keys$tasks, id)
        con$HDEL(keys$tasks_status, id)
        con$HDEL(keys$tasks_result, id)
      })
      as.logical(ret[seq_along(id)])
    }
  ))

##' Create an rrqueue queue
##' @title Create an rrqueue queue
##' @param name Queue name
##' @param packages Character vector of packages to load
##' @param sources Character vector of files to source
##' @param con Connection to a redis database
##' @export
queue <- function(name, packages=NULL, sources=NULL, con=NULL) {
  .R6_queue$new(name, packages, sources, con)
}

## TODO: Refuse to run if redis version is not OK
