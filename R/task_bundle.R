##' Create a task bundle.  Generally these are not created manually,
##' but this page serves to document methods that bundles have.
##' @title Create a task bundle
##' @param obj An observer or queue object
##' @param tasks A list of tasks
##' @param groups Optional vector of groups, may be dropped soon
##' @param names Optional vector of names to label output with.
##' @export
task_bundle <- function(obj, tasks, groups=NULL, names=NULL) {
  ## TODO: What is groups used for here?  Seems no longer needed?
  .R6_task_bundle$new(obj, tasks, groups, names)
}

.R6_task_bundle <- R6::R6Class(
  "task_bundle",

  public=
    list(
      obj=NULL,
      tasks=NULL,
      task_ids=NULL,
      key_complete=NULL,
      groups=NULL,
      names=NULL,
      con=NULL,
      keys=NULL,
      results=NULL,
      done=NULL,

      initialize=function(obj, tasks, groups, names) {
        self$con <- obj$con
        self$keys <- obj$keys
        self$obj <- obj

        n <- length(tasks)
        self$task_ids <- vcapply(tasks, "[[", "id")
        self$tasks <- setNames(tasks, self$task_ids)

        self$key_complete <- unique(vcapply(tasks, "[[", "key_complete"))

        if (is.null(groups)) {
          groups <- obj$tasks_lookup_group(self$task_ids)
        }
        self$groups <- groups

        self$names <- names
        self$results <- setNames(vector("list", n), self$task_ids)
        self$update_results()
      },

      status=function() {
        self$obj$tasks_status(self$task_ids, follow_redirect=TRUE)
      },

      update_results=function() {
        status <- self$status()
        self$done <- !(status == TASK_PENDING | status == TASK_RUNNING |
                         status == TASK_ORPHAN)
        if (any(self$done)) {
          get1 <- function(id) {
            self$obj$task_result(id, follow_redirect=TRUE, sanitise=TRUE)
          }
          ids <- self$task_ids[self$done]
          self$results[self$done] <- lapply(ids, get1)
        }
      },

      wait=function(timeout=1, progress_bar=TRUE, maxit=Inf) {
        if (timeout < 1) {
          stop("timeout must be at least 1")
        }
        self$update_results()
        p <- progress(total=length(self$tasks), show=progress_bar)

        p(sum(self$done))
        i <- 1L
        while (!all(self$done)) {
          id <- self$fetch1(timeout)
          if (is.null(id)) {
            p(0)
          } else {
            p(1)
          }

          i <- i + 1L
          if (i > maxit) {
            stop("Exceeded maximum number of iterations")
          }
        }

        setNames(self$results, self$names)
      },

      fetch1=function(timeout) {
        if (as.integer(timeout) > 0) {
          task_id <- self$con$BLPOP(self$key_complete, timeout)
          if (!is.null(task_id)) {
            task_id <- task_id[[2]]
          }
        } else {
          ## Way more complicated, simulation of BLPOP with no timeout
          ## on multiple lists.  Not anything safe.
          for (k in self$key_complete) {
            task_id <- self$con$LPOP(k)
            if (!is.null(task_id)) {
              break
            }
          }
        }

        if (!is.null(task_id)) {
          res <- self$tasks[[task_id]]$result(follow_redirect=TRUE,
                                              sanitise=TRUE)
          ## NOTE: This conditional is needed to avoid deleting the
          ## element in results if we get a NULL results.
          if (!is.null(res)) {
            self$results[[task_id]] <- res
          }
          self$done[[task_id]] <- TRUE
        }
        task_id
      },

      delete_tasks=function() {
        invisible(self$obj$tasks_drop(self$task_ids))
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
  key_complete <- unique(vcapply(tasks, "[[", "key_complete"))
  task_bundle(obj, tasks, groups)
}
