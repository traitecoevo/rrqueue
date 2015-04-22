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

WorkerEnvironmentFailed <- function(worker, task_id, e) {
  msg <- sprintf("Environment load for %s failed:\n\t%s",
                 task_id, e$message)
  worker$log("ENVIR ERROR", task_id)
  message("\t", e$message)
  WorkerError(worker, msg,
              task_id=task_id, task_status=TASK_ENVIR_ERROR,
              call=e$call, class="WorkerEnvironmentFailed")
}

UnfetchableTask <- function(task_id, task_status) {
  structure(list(task_id=task_id,
                 task_status=task_status),
            class=c(class, "WorkerError", "error", "condition"))
}
