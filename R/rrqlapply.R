##' Parallel version of lapply using Redis queuing
##' @title Parallel version of lapply using Redis queuing
##' @param X A vector
##' @param FUN The name of a function to apply to each element of the
##' list.  \emph{this will change!}.
##' @param rrq An rrq object
##' @param period Period to poll for completed tasks.  Affects how
##' responsive the function is to quiting, mostly.
##' @param delete_tasks Delete tasks on successful finish?
##' @param progress_bar Display a progress bar?
##' @export
rrqlapply <- function(X, FUN, rrq, period=1, delete_tasks=FALSE,
                      progress_bar=TRUE) {
  obj <- rrqlapply_submit(X, FUN, rrq)
  tryCatch(rrqlapply_results(obj, period, delete_tasks, progress_bar),
           interrupt=function(e) obj)
}

##' @export
##' @rdname rrqlapply
rrqlapply_submit <- function(X, FUN, rrq) {
  ## There's some crazy logic needed here; will be sorted once we get
  ## environments correct; work towards this will appear in
  ## R/functions.R - in general we should look in the environment and
  ## see if we can find the correct function.
  if (!is.character(FUN)) {
    stop("currently need name of function, sorry")
  }

  i <- rrq$con$GET(rrq$keys$tasks_counter)
  i <- if (is.null(i)) 1L else (as.integer(i) + 1L)
  key_complete <- rrqueue_key_task_complete(rrq$queue_name, i)

  tasks <- vector("list", length(X))
  e <- environment()
  for (i in seq_along(X)) {
    expr <- call(FUN, X[[i]])
    tasks[[i]] <- rrq$enqueue_(expr, e, key_complete=key_complete)
  }

  names(tasks) <- vcapply(tasks, "[[", "id")

  ## Should return something slightly more classy than this so that it
  ## can be used directly as a proxy and can be indexed with JIT
  ## lookup of data.

  ## TODO: To make re-attaching to this easy we need to have another
  ## bit of data: "rrqlapply jobs" so that we can say "attach me to
  ## whatever is going please".
  list(rrq=rrq,
       key_complete=key_complete,
       tasks=tasks,
       names=names(X))
}

##' @export
##' @param obj result of \code{rrqlapply_submit}
##' @rdname rrqlapply
rrqlapply_results <- function(obj, period=1, delete_tasks=FALSE,
                              progress_bar=TRUE) {
  rrq <- obj$rrq
  key_complete <- obj$key_complete
  tasks <- obj$tasks
  task_ids <- names(tasks)
  n <- length(tasks)

  output <- setNames(vector("list", n), task_ids)

  status <- rrq$tasks_status(task_ids)
  ## TODO: This doesn't deal with redirect yet; should have a
  ## tasks_status() function that follows redirects I guess?
  done <- !(status == TASK_PENDING | status == TASK_RUNNING)

  ## This is going to do weird things for *some* errors;
  ##   TASK_ORPHAN
  ##   TASK_REDIRECT
  ##   TASK_MISSING
  ##   TASK_ENVIR_ERROR
  ## really, we can only fetch TASK_COMPLETE and TASK_ERROR here.
  ## TODO: need a status function that does redirect I think.
  if (any(done)) {
    ok <- status == TASK_COMPLETE | status == TASK_ERROR
    nok <- done & !ok
    if (any(ok)) {
      output[ok] <- rrq$tasks_result(task_ids[ok])
    }
    if (any(nok)) {
      ## TODO: would be nice to wrap this into tasks_result_sane()
      output[nok] <- mapply(UnfetchableJob, task_ids[nok], status[nok])
    }
  }

  p <- progress(total=n, show=progress_bar)

  while (!all(done)) {
    res <- rrq$con$BLPOP(key_complete, period)
    if (!is.null(res)) {
      ## TODO: this needs result_sane() too...
      task_id <- res[[2]]
      output[[task_id]] <- tasks[[task_id]]$result()
      done[[task_id]] <- TRUE
      p()
    }
  }
  names(output) <- obj$names

  ## TODO:
  ## Deal with case where this gets called twice by accident and the
  ## result is an error second time; can do that with a reference
  ## class for the object, and / or making cleaning optional.
  if (delete_tasks) {
    rrq$tasks_drop(task_ids)
  }

  output
}
