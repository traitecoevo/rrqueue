StopWorker <- function(message="Shut down worker", call=NULL) {
  class <- c("StopWorker", "error", "condition")
  structure(list(message = as.character(message), call = call),
            class = class)
}

WorkerError <- function(worker, message, ...,
                        task_id=NULL, class=character(0)) {
  structure(list(worker=worker, task_id=task_id, ...,
                 message=message, call=NULL),
            class=c(class, "WorkerError", "error", "condition"))
}

WorkerTaskMissing <- function(worker, task_id, call=NULL) {
  WorkerError(sprintf("Task %s/%s not found", worker$name, task_id),
              task_id=task_id, class="WorkerTaskMissing")
}

WorkerEnvironmentFailed <- function(worker, task_id, message, call=NULL) {
  WorkerError(sprintf("Task %s/%s failed", worker$name, task_id),
              task_id=task_id, class="WorkerEnvironmentFailed")
}

WorkerTaskFailed <- function(worker, task_id, message, call=NULL) {
  WorkerError(sprintf("Task %s/%s failed", worker$name, task_id),
              task_id=task_id, class="WorkerEnvironmentFailed")
}
