##' Creates an observer for an rrqueue.  This is the "base class" for
##' a couple of different objects in rrqueue; notably the
##' \code{\link{queue}} object.  So any method listed here also works
##' within \code{queue} objects.
##'
##' Most of the methods of the \code{observer} object are extremely
##' simple and involve fetching information from the database about
##' the state of tasks, environments and workers.
##'
##' The method and argument names try to give hints about the sort of
##' things they expect; a method asking for \code{task_id} expects a
##' single task identifier, while those asking for \code{task_ids}
##' expect a vector of task identifiers (and if they have a default
##' \code{NULL} then will default to returning information for
##' \emph{all} task identifiers).  Similarly, a method starting
##' \code{task_} applies to one task while a method starting
##' \code{tasks_} applies to multiple.
##'
##' @template observer_methods
##' @title Creates an observer for an rrqueue
##' @param queue_name Name of the queue
##' @param redis_host Redis hostname
##' @param redis_port Redis port number
##' @param config Configuration file of key/value pairs in yaml
##'   format.  See the package README for an example.  If given,
##'   additional arguments to this function override values in the
##'   file which in turn override defaults of this function.
##' @export
observer <- function(queue_name,
                     redis_host="127.0.0.1", redis_port=6379,
                     config=NULL) {
  if (!is.null(config)) {
    given <- as.list(sys.call())[-1] # -1 is the function name
    dat <- modifyList(load_config(config), given)
    if (is.null(dat$queue_name)) {
      stop("queue_name must be given or specified in config")
    }
    observer(dat$queue_name, dat$redis_host, dat$redis_port, NULL)
  } else {
    .R6_observer$new(queue_name, redis_host, redis_port)
  }
}

## I think the correct design pattern is one that is totally dense
## that takes queue_name, redis_host, redis_port and which sets up the
## connection and keys.
##
## Other things can either inherit from this or compose with it.
##
## NOTE: There are no methods here that modify the queue.
.R6_observer <- R6::R6Class(
  "observer",
  public=list(
    queue_name=NULL,
    con=NULL,
    keys=NULL,
    files=NULL,
    objects=NULL,

    initialize=function(queue_name, redis_host, redis_port) {
      self$queue_name <- queue_name
      self$con <- redis_connection(redis_host, redis_port)
      self$keys <- rrqueue_keys(self$queue_name)
      self$files <- file_cache(self$keys$files, self$con)
      self$objects <- object_cache(self$keys$objects, self$con)
    },

    ## 1. Tasks:
    tasks_list=function() {
      tasks_list(self$con, self$keys)
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
      tasks_envir(self$con, self$keys, task_ids)
    },
    task_get=function(task_id) {
      task(self, task_id)
    },
    task_result=function(task_id, follow_redirect=FALSE, sanitise=FALSE) {
      task_result(self$con, self$keys, task_id, follow_redirect, sanitise)
    },

    ## (task groups)
    tasks_groups_list=function() {
      tasks_groups_list(self$con, self$keys)
    },
    tasks_in_groups=function(groups) {
      tasks_in_groups(self$con, self$keys, groups)
    },
    tasks_lookup_group=function(task_ids=NULL) {
      tasks_lookup_group(self$con, self$keys, task_ids)
    },
    task_bundle_get=function(groups=NULL, task_ids=NULL) {
      task_bundle_get(self, groups, task_ids)
    },

    ## 2: environments
    envirs_list=function() {
      envirs_list(self$con, self$keys)
    },
    envirs_contents=function(envir_ids=NULL) {
      envirs_contents(self$con, self$keys, envir_ids)
    },
    envir_workers=function(envir_id, worker_ids=NULL) {
      envir_workers(self$con, self$keys, envir_id, worker_ids)
    },

    ## 3: workers
    workers_len=function() {
      workers_len(self$con, self$keys)
    },
    workers_list=function() {
      workers_list(self$con, self$keys)
    },
    workers_list_exited=function() {
      workers_list_exited(self$con, self$keys)
    },
    workers_status=function(worker_ids=NULL) {
      workers_status(self$con, self$keys, worker_ids)
    },
    workers_times=function(worker_ids=NULL, unit_elapsed="secs") {
      workers_times(self$con, self$keys, worker_ids, unit_elapsed)
    },
    workers_log_tail=function(worker_ids=NULL, n=1) {
      workers_log_tail(self$con, self$keys, worker_ids, n)
    },
    ## NOTE: this returns data that is not necessarily fresh:
    workers_info=function(worker_ids=NULL) {
      workers_info(self$con, self$keys, worker_ids)
    },
    worker_envir=function(worker_id) {
      worker_envir(self$con, self$keys, worker_id)
    },
    workers_running=function(worker_ids=NULL) {
      workers_running(self$con, self$keys, worker_ids)
    }
    ))
