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
rrqlapply <- function(X, FUN, rrq, ...,
                      period=1, delete_tasks=FALSE,
                      progress_bar=TRUE, env=parent.frame()) {
  obj <- rrqlapply_submit(X, FUN, rrq, ..., env=env)
  tryCatch(rrqlapply_results(obj, period, delete_tasks, progress_bar),
           interrupt=function(e) obj)
}

##' @export
##' @rdname rrqlapply
rrqlapply_submit <- function(X, FUN, rrq, ..., env=parent.frame()) {
  ## This is hopefully going to be enough:
  dat <- match_fun_rrqueue(FUN, env, rrq$envir)
  if (dat[[1]] == "") {
    fun <- as.name(dat[[2]])
  } else {
    fun <- call("::", as.name(dat[[1]]), as.name(dat[[2]]))
  }
  DOTS <- list(...)

  i <- rrq$con$GET(rrq$keys$tasks_counter)
  i <- if (is.null(i)) 1L else (as.integer(i) + 1L)
  key_complete <- rrqueue_key_task_complete(rrq$queue_name, i)

  tasks <- vector("list", length(X))
  e <- environment()
  for (i in seq_along(X)) {
    expr <- as.call(c(list(fun, X[[i]]), DOTS))
    tasks[[i]] <- rrq$enqueue_(expr, e, key_complete=key_complete)
  }

  names(tasks) <- vcapply(tasks, "[[", "id")

  ## Should return something slightly more classy than this so that it
  ## can be used directly as a proxy and can be indexed with JIT
  ## lookup of data.

  ## TODO: To make re-attaching to this easy we need to have another
  ## bit of data: "rrqlapply jobs" so that we can say "attach me to
  ## whatever is going please".
  ret <- list(rrq=rrq,
              key_complete=key_complete,
              tasks=tasks,
              names=names(X))
  class(ret) <- "rrqlapply_tasks"
  ret
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

  ## TODO: It's really not that clear what to do here in terms of
  ## redirecting things.  I think that this is going to work, but
  ## testing it is going to be really hard.
  ##
  ## However, because the collecting function is designed to called
  ## multiple times without problem, we can detect orphaned tasks,
  ## requeue them, and then re-collect.
  status <- rrq$tasks_status(task_ids, follow_redirect=TRUE)
  done <- !(status == TASK_PENDING | status == TASK_RUNNING |
              status == TASK_ORPHAN)
  if (any(done)) {
    output[done] <-
      rrq$tasks_result(task_ids[done], follow_redirect=TRUE, sanitise=TRUE)
  }

  p <- progress(total=n, show=progress_bar)
  p(0) # force display of the bar

  while (!all(done)) {
    res <- rrq$con$BLPOP(key_complete, period)
    if (is.null(res)) {
      p(0)
    } else {
      task_id <- res[[2]]
      output[[task_id]] <-
        tasks[[task_id]]$result(follow_redirect=TRUE, sanitise=TRUE)
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
