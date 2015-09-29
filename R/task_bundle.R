##' Create a task bundle.  Generally these are not created manually,
##' but this page serves to document what task bundles are and the
##' methods that they have.
##'
##' A task bundle exists to group together tasks that are related.  It
##' is possible for a task to belong to multiple bundles.
##'
##' @template task_bundle_methods
##'
##' @title Create a task bundle
##' @param obj An observer or queue object
##' @param tasks A list of tasks
##' @param groups Optional vector of groups.  If given, then additional
##'    tasks can be added to the bundle if they share the same group names.
##' @param names Optional vector of names to label output with.
##' @export
task_bundle <- function(obj, tasks, groups=NULL, names=NULL) {
  ## TODO: What is groups used for here?  Seems no longer needed?
  .R6_task_bundle$new(obj, tasks, groups, names)
}

## TODO: Next, make an automatically updating version.
.R6_task_bundle <- R6::R6Class(
  "task_bundle",

  public=
    list(
      obj=NULL,
      tasks=NULL,
      key_complete=NULL,
      groups=NULL,
      names=NULL,
      con=NULL,
      keys=NULL,

      initialize=function(obj, tasks, groups, names) {
        self$con <- obj$con
        self$keys <- obj$keys
        self$obj <- obj
        self$tasks <- setNames(tasks, vcapply(tasks, "[[", "id"))

        self$key_complete <- unique(vcapply(tasks, "[[", "key_complete"))
        self$groups <- groups

        if (!is.null(names) && length(names) != length(tasks)) {
          stop("Incorrect length names")
        }
        self$names <- names
      },

      ids=function() {
        names(self$tasks)
      },

      update_groups=function() {
        task_ids <- setdiff(tasks_in_groups(self$con, self$keys, self$groups),
                            self$ids())
        if (length(task_ids)) {
          tasks <- setNames(lapply(task_ids, self$obj$task_get), task_ids)
          self$tasks <- c(self$tasks, tasks)
          self$key_complete <- union(self$key_complete,
                                     unique(vcapply(tasks, "[[", "key_complete")))
          ## Can't deal with this for now :(
          self$names <- NULL
        }
        invisible(task_ids)
      },

      status=function(follow_redirect=FALSE) {
        self$obj$tasks_status(self$ids(), follow_redirect=follow_redirect)
      },
      results=function(follow_redirect=FALSE) {
        self$wait(0, 0, FALSE, follow_redirect)
      },
      wait=function(timeout=60, time_poll=1, progress_bar=TRUE, follow_redirect=FALSE) {
        task_bundle_wait(self, timeout, time_poll, progress_bar, follow_redirect)
      },
      times=function(unit_elapsed="secs") {
        obj$task_times(self$ids(), unit_elapsed)
      },


      delete_tasks=function() {
        invisible(self$obj$tasks_drop(self$ids()))
      }))


## There are a bunch of ways of getting appropriate things here:
task_bundle_get <- function(obj, groups=NULL, task_ids=NULL) {
  if (!xor(is.null(task_ids), is.null(groups))) {
    stop("Exactly one of task_ids or groups must be given")
  }
  if (is.null(groups)) {
    groups <- obj$tasks_lookup_group(task_ids)
  } else {
    task_ids <- obj$tasks_in_groups(groups)
  }

  tasks <- lapply(task_ids, obj$task_get)
  names(tasks) <- task_ids
  task_bundle(obj, tasks, groups)
}


task_bundle_wait <- function(obj, timeout, time_poll, progress_bar, follow_redirect) {
  assert_integer_like(time_poll)
  task_ids <- obj$ids()
  status <- obj$status()
  done <- !(status == TASK_PENDING | status == TASK_RUNNING |
              status == TASK_ORPHAN)

  ## Immediately collect all results:
  results <- named_list(task_ids)
  if (any(done)) {
    results[done] <- lapply(obj$tasks[done],
                            function(t) t$result(follow_redirect))
  }

  cleanup <- function(results, names) {
    if (!is.null(names)) {
      names(results) <- names
    }
    results
  }
  if (all(done)) {
    return(cleanup(results, obj$names))
  } else if (timeout == 0) {
    stop("Tasks not yet completed; can't be immediately returned")
  }

  p <- progress(total=length(obj$tasks), show=progress_bar)
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")

  p(sum(done))
  i <- 1L
  while (!all(done)) {
    if (Sys.time() - t0 > timeout) {
      stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                   sum(!done), length(done)))
    }
    res <- task_bundle_fetch1(obj, time_poll, follow_redirect)
    if (is.null(res)) {
      p(0)
    } else {
      p(1)
      task_id <- res[[1]]
      value <- res[[2]]
      done[[task_id]] <- TRUE
      ## NOTE: This conditional is needed to avoid deleting the
      ## element in results if we get a NULL value.
      if (!is.null(value)) {
        results[[task_id]] <- value
      }
    }

  }
  cleanup(results, obj$names)
}

task_bundle_fetch1 <- function(bundle, timeout, follow_redirect) {
  if (as.integer(timeout) > 0) {
    res <- bundle$con$BLPOP(bundle$key_complete, timeout)
  } else {
    res <- lpop_mult(bundle$con, bundle$key_complete)
  }
  if (!is.null(res)) {
    id <- res[[2]]
    list(id, bundle$obj$task_result(id, follow_redirect=follow_redirect))

  } else {
    NULL
  }
}
