##' @section Methods:
##'
##' \describe{
##' \item{\code{status}}{
##'   Returns a scalar character indicating the task status.
##'
##'   \emph{Usage:}
##'   \code{status(follow_redirect = FALSE)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{follow_redirect}}{
##'       should we follow redirects to get the status of any requeued task?
##'     }
##'   }
##'
##'   \emph{Value}:
##'   Scalar character.  Possible values are
##'   \describe{
##'   \item{\code{PENDING}}{queued, but not run by a worker}
##'   \item{\code{RUNNING}}{being run on a worker, but not complete}
##'   \item{\code{COMPLETE}}{task completed successfully}
##'   \item{\code{ERROR}}{task completed with an error}
##'   \item{\code{ORPHAN}}{task orphaned due to loss of worker}
##'   \item{\code{REDIRECT}}{orphaned task has been redirected}
##'   \item{\code{MISSING}}{task not known (deleted, or never existed)}
##'   }
##' }
##' \item{\code{result}}{
##'   Fetch the result of a task, so long as it has completed.
##'
##'   \emph{Usage:}
##'   \code{result(follow_redirect = FALSE)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{follow_redirect}}{
##'       should we follow redirects to get the status of any requeued task?
##'     }
##'   }
##' }
##' \item{\code{wait}}{
##'   Like \code{result}, but will wait until the task is complete.  In order to preserve the \code{key_complete} for anything that might be listening for it (and to avoid collision with anything else writing to that key), this function repeatedly polls the database. Over a slow connection you may want to increase the \code{every} parameter.
##'
##'   \emph{Usage:}
##'   \code{wait(timeout = , every = 0.05)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{timeout}}{
##'       Length of time, in seconds, to wait.  A value of zero will not wait.  Infinite times are possible and can be escaped by pressing Ctrl-C or Escape (depending on platform).
##'     }
##'
##'     \item{\code{every}}{
##'       How often, in seconds, to poll for results
##'     }
##'   }
##' }
##' \item{\code{expr}}{
##'   returns the expression stored in the task
##'
##'   \emph{Usage:}
##'   \code{expr(locals = FALSE)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{locals}}{
##'       Logical, indicating if the local variables associated with the expression should also be retuned.  If \code{TRUE}, then local variables used in the expression will be returned in a \emph{attribute} of the expression \code{envir}.
##'     }
##'   }
##'
##'   \emph{Value}:
##'   A quoted expression (a language object).  Turn this into a string with deparse.  If \code{locals} was \code{TRUE} there will be an environment attribute with local variables included.
##' }
##' \item{\code{envir}}{
##'   returns the environment identifier for the task
##'
##'   \emph{Usage:}
##'   \code{envir()}
##' }
##' \item{\code{times}}{
##'   returns a summar of times associated with this task.
##'
##'   \emph{Usage:}
##'   \code{times(unit_elapsed = "secs")}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{unit_elapsed}}{
##'       Unit to use in computing elapsed times.  The default is to use "secs".  This is passed through to \code{\link{difftime}} so the units there are available and are "auto", "secs", "mins", "hours", "days", "weeks".
##'     }
##'   }
##'
##'   \emph{Value}:
##'   A one row \code{data.frame} with columns
##'   \describe{
##'   \item{\code{submitted}}{Time the task was submitted}
##'   \item{\code{started}}{Time the task was started, or \code{NA} if waiting}
##'   \item{\code{finished}}{Time the task was completed, or \code{NA}
##'   if waiting or running}
##'   \item{\code{waiting}}{Elapsed time spent waiting}
##'   \item{\code{running}}{Elapsed time spent running, or \code{NA} if waiting}
##'   \item{\code{idle}}{Elapsed time since finished, or \code{NA}
##'   if waiting or running}
##'   }
##' }
##' }
