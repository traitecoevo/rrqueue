##' Parallel version of lapply using Redis queuing
##' @title Parallel version of lapply using Redis queuing
##' @param X A vector
##' @param FUN The name of a function to apply to each element of the
##' list.  \emph{this will change!}.
##' @param rrq An rrq object
##' @param ... Additional arguments passed to \code{FUN}
##' @param period Period to poll for completed tasks.  Affects how
##' responsive the function is to quiting, mostly.
##' @param delete_tasks Delete tasks on successful finish?
##' @param progress_bar Display a progress bar?
##' @param env Environment to look in.
##' @export
rrqlapply <- function(X, FUN, rrq, ...,
                      period=1, delete_tasks=FALSE,
                      progress_bar=TRUE, env=parent.frame()) {
  ## TODO: I've set progress_bar to be true on both submitting and
  ## retrieving, but the submit phase *should* be fast enough that
  ## it's not necessary.  That's not true if we're running Redis over
  ## a slow connection though (which we do with the clusterous
  ## approach).  This adds some overhead but I think it'll do for now.
  obj <- rrqlapply_submit(X, FUN, rrq, ..., progress_bar=progress_bar, env=env)
  tryCatch(rrqlapply_results(obj, period, delete_tasks, progress_bar),
           interrupt=function(e) obj)
}

##' @export
##' @rdname rrqlapply
rrqlapply_submit <- function(X, FUN, rrq, ...,
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
  p <- progress(total=n, show=progress_bar, prefix="submitting: ")
  for (i in seq_len(n)) {
    expr <- as.call(c(list(fun, X[[i]]), DOTS))
    tasks[[i]] <- rrq$enqueue_(expr, e, key_complete=key_complete)
    if (is.null(key_complete)) {
      key_complete <- tasks[[i]]$key_complete
    }
    p()
  }

  names(tasks) <- vcapply(tasks, "[[", "id")

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
    output[done] <- rrq$tasks_result(task_ids[done],
                                     follow_redirect=TRUE, sanitise=TRUE)
  }

  p <- progress(total=n, show=progress_bar)
  p(0) # force display of the bar

  ## NOTE: because of using BLPOP here there's no real reason why we
  ## can't use a vector for key_complete.  But at the same time, no
  ## real reason to do this.
  while (!all(done)) {
    res <- rrq$con$BLPOP(key_complete, period)
    if (is.null(res)) {
      p(0)
    } else {
      task_id <- res[[2]]
      tmp <- tasks[[task_id]]$result(follow_redirect=TRUE, sanitise=TRUE)
      if (!is.null(tmp)) {
        ## This conditional is needed to avoid deleting the element in
        ## output if we get a NULL output.
        output[[task_id]] <- tmp
      }
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

## This is hopefully going to be enough:
find_fun <- function(FUN, env, rrq) {
  dat <- match_fun_rrqueue(FUN, env, rrq$envir)
  if (dat[[1]] == "") {
    as.name(dat[[2]])
  } else {
    call("::", as.name(dat[[1]]), as.name(dat[[2]]))
  }
}
