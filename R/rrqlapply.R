rrqlapply <- function(x, fun, rrq) {
  ## There's some crazy logic needed here; will be sorted once we get
  ## environments correct; work towards this will appear in
  ## R/functions.R
  if (!is.character(fun)) {
    stop("currently need name of function, sorry")
  }

  i <- rrq$con$GET(rrq$keys$tasks_counter)
  i <- if (is.null(i)) 1L else (as.integer(i) + 1L)
  key_complete <- rrqueue_key_task_complete(rrq$queue_name, i)

  tasks <- vector("list", length(x))
  e <- environment()
  for (i in seq_along(x)) {
    expr <- call(fun, x[[i]])
    tasks[[i]] <- rrq$enqueue_(expr, e, key_complete=key_complete)
  }
  tasks_id <- vcapply(tasks, "[[", "id")
  output <- vector("list", length(x))
  done <- logical(length(x))
  names(done) <- names(output) <- tasks_id

  ## TODO: use Gabor's new progress bars here? (gaborcsardi/progress)
  timeout <- 1
  repeat {
    res <- rrq$con$BLPOP(key_complete, timeout)
    if (is.null(res)) {
      message(".", appendLF=FALSE)
    } else {
      id <- res[[2]]
      output[[id]] <- rrq$tasks_collect(id)
      rrq$tasks_drop(id)
      done[[id]] <- TRUE
      if (all(done)) {
        break
      }
    }
  }
  message("done")
  output
}
