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
    queue_name=NULL,
    id=NULL,
    keys=NULL,
    key_complete=NULL,

    ## TODO: rather than taking {con, queue_name} we could take the
    ## queue object, from which we can get {con, queue_name, keys}
    ## directly.  So long as this is only driven by the controller
    ## we're free to make that change without breaking anything.
    ##
    ## TODO: could have key_complete be NULL by default in which case
    ## we can get it with
    ##   con$HGET(keys$tasks_complete, id)
    initialize=function(con, queue_name, id, key_complete=NULL) {
      self$con <- con
      self$queue_name <- queue_name
      self$id  <- as.character(id)
      self$keys <- rrqueue_keys(queue_name)
      if (is.null(key_complete)) {
        key_complete <- con$HGET(self$keys$tasks_complete, id)
      }
      self$key_complete <- key_complete
    },

    ## TODO: new methods:
    ##   - drop / cancel / kill [hmm]
    ##   - expr
    ##   - locals

    ## TODO: These could be active bindings?
    status=function() {
      status <- self$con$HGET(self$keys$tasks_status, self$id)
      if (is.null(status)) {
        status <- TASK_MISSING
      }
      status
    },

    result=function() {
      status <- self$status()
      if (status == TASK_MISSING) {
        stop("task does not exist")
      } else if (status != TASK_COMPLETE) {
        stop("task is incomplete")
      }
      string_to_object(self$con$HGET(self$keys$tasks_result, self$id))
    },

    envir=function() {
      self$con$HGET(self$keys$tasks_envir, self$id)
    }
  ))

task <- function(con, queue_name, id, key_complete) {
  .R6_task$new(con, queue_name, id, key_complete)
}
