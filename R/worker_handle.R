.R6_worker_handle <- R6::R6Class(
  "worker_handle",

  public=list(
    con=NULL,
    queue_name=NULL,
    id=NULL,
    keys=NULL,

    initialize=function(con, queue_name, id) {
      assert_inherits(con, "redis_api")
      assert_scalar_character(id)
      assert_scalar_character(queue_name)
      self$con <- con
      self$queue_name <- queue_name
      self$id <- id
      self$keys <- rrqueue_keys(self$queue_name, worker_name=self$id)
    },
    status=function() {
      workers_status(self$con, self$keys, self$id)
    },
    times=function() {
      workers_times(self$con, self$keys, self$id)
    },
    task_id=function() {
      worker_task_id(self$con, self$keys, self$id)
    },
    task_get=function() {
      worker_task_get(self$con, self$keys, self$id)
    },
    log_tail=function(n=1) {
      worker_log_tail(self$con, self$keys, self$id, n)
    },
    send_message=function(content) {
      self$con$RPUSH(self$keys$message, content)
    },
    messages=function() {
      as.character(self$con$LRANGE(self$keys$message, 0, -1))
    }
  ))

worker_handle <- function(con, queue_name, id) {
  .R6_worker_handle$new(con, queue_name, id)
}
