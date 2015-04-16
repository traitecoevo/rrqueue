## This actually duplicates most of controller; controller might end
## up based on this.
##
## I think the correct design pattern is one that is totally dense
## that takes queue_name, redis_host, redis_port and which sets up the
## connection and keys.
##
## Other things can either inherit from this or compose with it.
##
## TODO: probably need something like this across queues?
.R6_observer <- R6::R6Class(
  "observer",
  public=list(
    queue_name=NULL,
    con=NULL,
    keys=NULL,
    objects=NULL,

    initialize=function(queue_name, redis_host, redis_port) {
      self$queue_name <- queue_name
      self$con <- redis_connection(redis_host, redis_port)
      self$keys <- rrqueue_keys(self$queue_name)
      self$objects <- object_cache(self$con, self$keys$objects)
    },

    ## 1. Tasks:
    tasks_list=function() {
      as.character(self$con$LRANGE(self$keys$tasks_id, 0, -1))
    },

    tasks_len=function() {
      self$con$LLEN(self$keys$tasks_id)
    },

    tasks_status=function(task_ids=NULL) {
      from_redis_hash(self$con, self$keys$tasks_status, task_ids,
                      as.character, TASK_MISSING)
    },

    ## How many jobs are still running / completed / failed
    tasks_overview=function() {
      lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
      status <- self$tasks_status()
      lvls <- c(lvls, setdiff(unique(status), lvls))
      table(factor(status, lvls))
    },

    tasks_times=function(task_ids=NULL, unit_elapsed="secs") {
      f <- function(key) {
        from_redis_hash(self$con, key, task_ids, redis_time_to_r)
      }
      ret <- data.frame(submitted = f(self$keys$tasks_time_sub),
                        started   = f(self$keys$tasks_time_beg),
                        finished  = f(self$keys$tasks_time_end))
      ret$waiting <- as.numeric(ret$started  - ret$submitted, unit_elapsed)
      ret$running <- as.numeric(ret$finished - ret$started,   unit_elapsed)
      ret
    },

    tasks_envir=function(task_ids=NULL) {
      from_redis_hash(self$con, self$keys$tasks_envir, tasks_ids)
    },

    task_get=function(task_id) {
      key_complete <- self$con$HGET(self$keys$tasks_complete, task_id)
      task(self$con, self$queue_name, task_id, key_complete)
    },

    ## TODO: This is going to factor out into task.R
    task_expr=function(task_id, locals=FALSE) {
      task_expr <- self$con$HGET(self$keys$tasks_expr, task_id)
      if (locals) {
        e <- new.env(parent=baseenv())
        expr <- restore_expression(task_expr, e, obj$objects)
        attr(expr, "envir") <- e
        expr
      } else {
        restore_expression_simple(task_expr)
      }
    },

    ## TODO: Factor out into task.R
    task_result=function(task_id) {
      status <- self$tasks_status(id)
      if (status == TASK_MISSING) {
        stop("task does not exist")
      } else if (status == TASK_REDIRECT) {
        task2_id <- self$con$HGET(self$keys$tasks_redirect, task_id)
        return(self$task_result(task2_id))
      } else if (status != TASK_COMPLETE) {
        stop("task is incomplete")
      }
      string_to_object(self$con$HGET(self$keys$tasks_result, id))
    },

    tasks_expr=function(task_ids, ...) {
      setNames(lapply(task_ids, self$task_expr, ...), task_ids)
    },

    tasks_result=function(task_ids) {
      setNames(lapply(task_ids, self$task_result), task_ids)
    },

    ## 2: environments
    envirs_list=function() {
      as.character(self$con$HKEYS(self$keys$envirs_contents))
    },

    envirs_contents=function(envir_ids=NULL) {
      dat <- from_redis_hash(self$con, self$keys$envirs_contents, envir_ids)
      lapply(dat, string_to_object)
    },

    envirs_tasks=function(envir_ids=NULL) {
      ## TODO: should be done with HSCAN, and not implemented for
      ## efficiency.  There's bound to be a way of doing this natively
      ## in Redis.
      if (is.null(envir_ids)) {
        envir_ids <- self$envirs_list()
      }
      tmp <- self$tasks_envir()
      split(names(tmp), envir_ids[match(tmp, envir_ids)])
    },

    ## 3: workers
    workers_len=function() {
      ## NOTE: this is going to be an *estimate* because there might
      ## be old workers floating around.
      ##
      ## TODO: Drop orphan workers here.
      self$con$SCARD(self$keys$workers_name)
    },

    workers=function() {
      as.character(self$con$SMEMBERS(self$keys$workers_name))
    },

    workers_status=function() {
      from_redis_hash(self$con, self$keys$workers_status)
    },

    workers_status_time=function() {
      w <- self$workers()
      h <- rrqueue_key_worker_heartbeat(self$queue_name, w)
      m <- as.numeric(vcapply(h, self$con$GET))
      t <- unname(vnapply(h, self$con$PTTL)) / 1000
      a <- as.numeric(redis_time(self$con)) -
        self$worker_log_tail(w, 1)$time
      data.frame(worker_id=w,
                 expire_max=m,
                 expire=t,
                 last_seen=m - t,
                 last_action=a)
    },

    worker_log_tail=function(worker_id, n=1) {
      log_key <- rrqueue_key_worker_log(self$queue_name, worker_id)
      parse_worker_log(as.character(self$con$LRANGE(log_key, -n, -1)))
    },

    workers_log_tail=function(worker_ids=NULL, n=1) {
      if (is.null(worker_ids)) {
        worker_ids <- self$workers()
      }
      tmp <- lapply(worker_ids, self$worker_log_tail, n)
      do.call("rbind", tmp, quote=TRUE)
    }
    ))

observer <- function(queue_name,
                     redis_host="127.0.0.1", redis_port=6379) {
  .R6_observer$new(queue_name, redis_host, redis_port)
}
