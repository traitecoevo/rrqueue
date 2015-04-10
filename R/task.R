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
    ##   - drop / cancel / kill
    ##   - expr
    ##   - locals

    ## TODO: These could be active bindings?
    status=function() {
      self$con$HGET(self$keys$tasks_status, self$id)
    },
    result=function() {
      self$con$HGET(self$keys$tasks_result, self$id)
    },
    envir=function() {
      self$con$HGET(self$keys$tasks_envir, self$id)
    }
  ))

task <- function(con, queue_name, id, key_complete) {
  .R6_task$new(con, queue_name, id, key_complete)
}
