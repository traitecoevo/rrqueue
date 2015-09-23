WorkerError <- function(worker, message, ...,
                        task_id=NULL,
                        task_status=NULL,
                        class=character(0),
                        call=NULL) {
  structure(list(worker=worker,
                 task_id=task_id,
                 task_status=task_status, ...,
                 message=message, call=call),
            class=c(class, "WorkerError", "error", "condition"))
}

## This happens with no task
WorkerStop <- function(worker, message) {
  WorkerError(worker, message, class="WorkerStop")
}

## TODO: these might log more than once?
WorkerTaskMissing <- function(worker, task_id) {
  msg <- sprintf("Task %s/%s not found", worker$name, task_id)
  worker$log("TASK_MISSING", task_id)
  WorkerError(worker, msg,
              task_id=task_id, task_status=TASK_MISSING,
              class="WorkerTaskMissing")
}

WorkerTaskError <- function(e) {
  class(e) <- c("WorkerTaskError", "try-error", class(e))
  e
}

UnfetchableTask <- function(task_id, task_status) {
  structure(list(task_id=task_id,
                 task_status=task_status),
            class=c(class, "WorkerError", "error", "condition"))
}
