WorkerError <- function(worker, message, ...,
                        task_id=NULL,
                        class=character(0),
                        call=NULL) {
  structure(list(worker=worker, task_id=task_id, ...,
                 message=message, call=call),
            class=c(class, "WorkerError", "error", "condition"))
}

WorkerStop <- function(worker, message) {
  WorkerError(worker, message, class="WorkerStop")
}

WorkerTaskMissing <- function(worker, task_id) {
  msg <- sprintf("Task %s/%s not found", worker$name, task_id)
  WorkerError(worker, msg, task_id=task_id, class="WorkerTaskMissing")
}

WorkerEnvironmentFailed <- function(worker, task_id, e) {
  msg <- sprintf("Environment load for %s failed\n:\t%s",
                 task_id, e$message)
  WorkerError(worker, msg, task_id=task_id, call=e$call,
              class="WorkerEnvironmentFailed")
}

WorkerTaskFailed <- function(worker, task_id, message) {
  msg <- sprintf("Task %s/%s failed", worker$name, task_id)
  WorkerError(worker, msg, task_id=task_id, class="WorkerTaskFailed")
}
