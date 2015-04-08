StopWorker <- function(message="Shut down worker", call=NULL) {
  class <- c("StopWorker", "error", "condition")
  structure(list(message = as.character(message), call = call),
            class = class)
}
