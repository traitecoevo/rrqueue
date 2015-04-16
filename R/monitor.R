## TODO: things to monitor specific completeness queues.
monitor_status <- function(obs) {
  message(monitor_status_workers(obs))
  message(monitor_status_tasks(obs))
}

monitor_status_workers <- function(obs) {
  cols <- cbind(c(WORKER_IDLE, "yellow"),
                c(WORKER_BUSY, "green"),
                c(WORKER_LOST, "red"))
  cols <- setNames(cols[2,], cols[1,])
  status <- obs$workers_status()
  monitor_status_string("workers", status, cols)
}

monitor_status_tasks <- function(obs) {
  cols <- cbind(c(TASK_PENDING, "grey"),
                c(TASK_RUNNING, "green"),
                c(TASK_COMPLETE, "blue"),
                c(TASK_ERROR, "red"),
                c(TASK_ENVIR_ERROR, "red"),
                c(TASK_ORPHAN, "hotpink"),
                c(TASK_REDIRECT, "orange"))
  cols <- setNames(cols[2,], cols[1,])
  status <- obs$tasks_status()
  monitor_status_string("tasks  ", status, cols, 1:4)
}

## TODO: Some of the nice colouring code from the worker would spice
## this up nicely.
monitor_status_string <- function(name, status, cols, i=NULL) {
  n <- table(factor(status, levels=names(cols)))
  if (!is.null(i)) {
    n <- n[i]
  }
  sprintf("%s [%s] %s",
          name,
          paste(n, collapse=" | "),
          pretty_blocks(status, cols))
}

## No need for a class - just run this function until it gets
## escaped.
monitor <- function(..., period=10) {
  obs <- observer(...)
  repeat {
    message(sprintf("[ %s ]", as.character(Sys.time())))
    monitor_status(obs)
    Sys.sleep(period)
  }
}
