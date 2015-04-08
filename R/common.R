rrqueue_keys <- function(queue) {
  list(workers        = sprintf("%s:workers",        queue),
       workers_status = sprintf("%s:workers:status", queue),
       workers_task   = sprintf("%s:workers:task",   queue),

       ## The tasks database:
       tasks          = sprintf("%s:tasks",         queue),
       tasks_counter  = sprintf("%s:tasks:counter", queue),
       tasks_id       = sprintf("%s:tasks:id",      queue),
       tasks_status   = sprintf("%s:tasks:status",  queue),
       tasks_result   = sprintf("%s:tasks:result",  queue),

       ## Environment setup:
       packages       = sprintf("%s:environment:packages", queue),
       sources        = sprintf("%s:environment:sources",  queue),
       objects        = sprintf("%s:environment:objects",  queue))
}

## Special key for worker-specific commands to be published to.
rrqueue_key_worker <- function(queue, worker) {
  sprintf("%s:worker:%s:message", queue, worker)
}

## These might change to serialisation/deserialisation, especially if
## the argument parsing bits get done at the same time.
save_expression <- function(expr) {
  deparse(expr)
}
restore_expression <- function(expr) {
  parse(text=expr)[[1]]
}

parse_worker_name <- function(str) {
  res <- strsplit(str, "::", fixed=TRUE)
  if (any(viapply(res, length) != 2)) {
    stop("parse error")
  }
  list(host=vcapply(res, "[[", 1),
       pid=vcapply(res, "[[", 2))
}
