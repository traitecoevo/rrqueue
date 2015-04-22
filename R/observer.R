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
      tasks_list(self$con, self$keys)
    },
    tasks_len=function() {
      tasks_len(self$con, self$keys)
    },
    tasks_status=function(task_ids=NULL, follow_redirect=FALSE) {
      tasks_status(self$con, self$keys, task_ids, follow_redirect)
    },
    tasks_overview=function() {
      tasks_overview(self$con, self$keys)
    },
    tasks_times=function(task_ids=NULL, unit_elapsed="secs") {
      tasks_times(self$con, self$keys, task_ids, unit_elapsed)
    },
    tasks_envir=function(task_ids=NULL) {
      ## TODO: tasks_env(self$con, self$keys, task_ids)
      from_redis_hash(self$con, self$keys$tasks_envir, task_ids)
    },
    task_get=function(task_id) {
      task_get(self$con, self$keys, task_id)
    },
    task_expr=function(task_id, locals=FALSE) {
      task_expr(self$con, self$keys, task_id, if (locals) self$objects)
    },
    task_result=function(task_id, follow_redirect=FALSE, sanitise=FALSE) {
      task_result(self$con, self$keys, task_id, follow_redirect, sanitise)
    },
    tasks_expr=function(task_ids, ...) {
      tasks_expr(self$con, self$keys, task_ids, ...)
    },
    tasks_result=function(task_ids, follow_redirect=FALSE, sanitise=FALSE) {
      setNames(lapply(task_ids, self$task_result, follow_redirect, sanitise),
               task_ids)
    },

    ## 2: environments
    envirs_list=function() {
      envir_list(self$con, self$keys)
    },
    envirs_contents=function(envir_ids=NULL) {
      envirs_contents(self$con, self$keys)
    },
    envirs_tasks=function(envir_ids=NULL) {
      envirs_tasks(self$con, self$keys)
    },

    ## 3: workers
    workers_len=function() {
      workers_len(self$con, self$keys)
    },
    workers_list=function() {
      workers_list(self$con, self$keys)
    },
    worker_get=function(worker_id) {
      worker_handle(self$con, self$queue_name, worker_id)
    },
    workers_status=function(worker_ids=NULL) {
      workers_status(self$con, self$keys, worker_ids)
    },
    workers_times=function(worker_ids=NULL) {
      workers_times(self$con, self$keys, worker_ids)
    },
    worker_log_tail=function(worker_id, n=1) {
      worker_log_tail(self$con, self$keys, worker_id, n)
    },
    workers_log_tail=function(worker_ids=NULL, n=1) {
      workers_log_tail(self$con, self$keys, worker_ids, n)
    }
    ))

observer <- function(queue_name,
                     redis_host="127.0.0.1", redis_port=6379) {
  .R6_observer$new(queue_name, redis_host, redis_port)
}
