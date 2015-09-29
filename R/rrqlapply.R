##' Parallel version of lapply using Redis queuing
##' @title Parallel version of lapply using Redis queuing
##' @param X A vector
##' @param FUN The name of a function to apply to each element of the
##' list.  \emph{this will change!}.
##' @param rrq An rrq object
##' @param ... Additional arguments passed to \code{FUN}
##' @param group Name of a group for generated task ids.  If not
##' included, an ID will be generated.
##' @param timeout Total length of time to wait for tasks to be
##'   completed.  The default is to wait forever (like \code{lapply}).
##'
##' @param time_poll Time to poll for tasks.  Must be an integer.
##'   Because of how the function is implemented, R will be
##'   unresponsive for this long each iteration (unless results are
##'   returned), so the default of 1s should be reasonable.
##' @param delete_tasks Delete tasks on successful finish?
##' @param progress_bar Display a progress bar?
##' @param env Environment to look in.
##' @export
rrqlapply <- function(X, FUN, rrq, ..., group=NULL,
                      timeout=Inf, time_poll=1, delete_tasks=FALSE,
                      progress_bar=TRUE, env=parent.frame()) {
  ## TODO: I've set progress_bar to be true on both submitting and
  ## retrieving, but the submit phase *should* be fast enough that
  ## it's not necessary.  That's not true if we're running Redis over
  ## a slow connection though (which we do with the clusterous
  ## approach).  This adds some overhead but I think it'll do for now.
  obj <- rrqlapply_submit(X, FUN, rrq, ..., group=group,
                          progress_bar=progress_bar, env=env)
  tryCatch(obj$wait(timeout, time_poll, progress_bar),
           interrupt=function(e) obj)
}

##' @export
##' @rdname rrqlapply
rrqlapply_submit <- function(X, FUN, rrq, ..., group=NULL,
                             progress_bar=TRUE, env=parent.frame()) {
  fun <- find_fun(FUN, env, rrq)
  DOTS <- list(...)

  n <- length(X)

  ## NOTE: the key_complete treatment here avoids possible race
  ## condition/implementation depenence by giving all tasks the same
  ## key_complete and making that shared with whatever the first gets
  ## (which is done via INCR).
  tasks <- vector("list", n)
  e <- environment()
  key_complete <- NULL
  group <- create_group(group, progress_bar)
  p <- progress(total=n, show=progress_bar, prefix="submitting: ")
  for (i in seq_len(n)) {
    expr <- as.call(c(list(fun, X[[i]]), DOTS))
    tasks[[i]] <- rrq$enqueue_(expr, e, key_complete=key_complete, group=group)
    if (is.null(key_complete)) {
      key_complete <- tasks[[i]]$key_complete
    }
    p()
  }
  task_bundle(rrq, tasks, group, names(X))
}

## This is hopefully going to be enough:
find_fun <- function(FUN, env, rrq) {
  dat <- match_fun_rrqueue(FUN, env, rrq$envir)
  if (dat[[1]] == "") {
    as.name(dat[[2]])
  } else {
    call("::", as.name(dat[[1]]), as.name(dat[[2]]))
  }
}
