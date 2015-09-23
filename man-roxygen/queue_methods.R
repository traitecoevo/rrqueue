##' @section Methods:
##'
##' \describe{
##' \item{\code{enqueue}}{
##'   The main queuing function.
##'
##'   \emph{Usage:}
##'   \code{enqueue(expr = , envir = .GlobalEnv, key_complete = NULL, group = NULL)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{expr}}{
##'       An unevaluated expression to be evaluated
##'     }
##'
##'     \item{\code{envir}}{
##'
##'       An environment in which local variables required to compute \code{expr} can be found.  These will be evaluated and added to the Redis database.
##'     }
##'
##'     \item{\code{key_complete}}{
##'
##'       an optional string representing the Redis key to write to when the task is complete.  You generally don't need to modify this, but is used in some higher-level functions (such as \code{link{rrqlapply}}) to keep track of task completions efficiently.
##'     }
##'
##'     \item{\code{group}}{
##'
##'       An optional human-readable "group" to add the task to. There are methods for addressing sets of tasks using this group.
##'     }
##'   }
##'
##'   \emph{Details:}
##'
##'   This method uses non standard evaluation and the \code{enqueue_} form may be prefereable for programming.
##'
##'   \emph{Value}:
##'
##'   invisibly, a \code{link{task}} object, which can be used to monitor the status of the task.
##' }
##' \item{\code{enqueue_}}{
##'
##'   The workhorse version of \code{enqueue} which uses standard evaluation and is therefore more suitable for programming.  All arguments are the same as \code{enqueue_} except for \code{eval}.
##'
##'   \emph{Usage:}
##'   \code{enqueue_(expr = , envir = .GlobalEnv, key_complete = NULL, group = NULL)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{expr}}{
##'       Either a character string or a language object.
##'     }
##'   }
##' }
##' \item{\code{requeue}}{
##'
##'   Re-queue a task that has been orphaned by worker failure.
##'
##'   \emph{Usage:}
##'   \code{requeue(task_id = )}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{task_id}}{
##'       Task id number
##'     }
##'   }
##'
##'   \emph{Details:}
##'
##'   If a worker fails (either an unhandled exception, R crash, network loss or machine loss) then if the worker was running a heartbeat process the task will eventually be flagged as orphaned.  If this happens then the task can be requeued. Functions for fetching and querying tasks take a \code{follow_redirect} argument which can be set to \code{TRUE} so that this new, requeued, task is found instead of the old task.
##'
##'   \emph{Value}:
##'
##'   invisibly, a \code{\link{task}} object.
##' }
##' \item{\code{send_message}}{
##'
##'   Send a message to one or more (or all) workers.  Messages can be used to retrieve information from workers, to shut them down and are useful in debugging.  See Details for possible messages and their action.
##'
##'   \emph{Usage:}
##'   \code{send_message(command = , args = NULL, worker_ids = NULL)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{command}}{
##'
##'       Name of the command to run; one of "PING", "ECHO", "EVAL", "STOP", "INFO", "ENVIR", "PUSH", "PULL", "DIR".  See Details.
##'     }
##'
##'     \item{\code{params}}{
##'
##'       Arguments to pass through to commands.  Some commands require arguments, others do not.  See Details.
##'     }
##'
##'     \item{\code{worker_ids}}{
##'
##'       Optional vector of worker ids to send the message to.  If this is omitted (or \code{NULL} then the message is broadcast to all workers that rrqueue knows about.
##'     }
##'   }
##'
##'   \emph{Details:}
##'
##'   The possible types of message are
##'   \code{PING}: send a "PING" to the worker.  It will respond by replying PONG to its stderr, to its log (see \code{observer} for how to access) and to the response queue.  Ignores any argument.
##'   \code{ECHO}: Like "PING", but the worker responds by echoing the string given.  Requires one argument.
##'   \code{EVAL}: Evaluate an arbitrary R expression as a string (e.g., \code{run_message("EVAL", "sin(1)")}).  The output is printed to stdout, the worker log and to the response queue.  Requires a single argument.
##'   \code{STOP}: Tell the worker to stop cleanly.  Ignores any argument.
##'   \code{INFO}: Refresh the worker info (see \code{workers_info} in \code{\link{observer}}.  Worker will print info to stderr, write it to the appropriate place in the database and return it in the response queue.  Ignores any argument.
##'   \code{ENVIR}: Tell the worker to try an load an environment, whose id is given as a single argument.  Requires a single argument.
##'   \code{PUSH}: Tell the worker to push files into the database.  The arguments should be a vector of filenames to copy.  The response queue will contain appropriate data for retrieving the files, but the interface here will change to make this nice to use.
##'   \code{PULL}: Tells the worker to pull files into its working directory.  Can be used to keep the worker in sync.
##'   \code{DIR}: Tell the worker to return directory contents and md5 hashes of files.
##'   After sending a message, there is no guarantee about how long it will take to process.  If the worker is involved in a long-running computation it will be unavailable to process the message. However, it will process the message before running any new task.
##'   The message id is worth saving.  It can be passed to the method \code{get_respones} to wait for and retrieve responses from one or more workers.
##'
##'   \emph{Value}:
##'
##'   The "message id" which can be used to retrieve messages with \code{has_responses}, \code{get_responses} and \code{get_response}.
##' }
##' \item{\code{has_responses}}{
##'
##'   Detect which workers have responses ready for a given message id.
##'
##'   \emph{Usage:}
##'   \code{has_responses(message_id = , worker_ids = NULL)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{message_id}}{
##'       id of the message (as returned by \code{send_message}
##'     }
##'
##'     \item{\code{worker_ids}}{
##'
##'       optional vector of worker ids, or \code{NULL} (the default) to try all known workers.
##'     }
##'   }
##'
##'   \emph{Value}:
##'
##'   A named logical vector; names are worker ids, the value is \code{TRUE} for each worker for which a response is ready and \code{FALSE} for workers where a response is not ready.
##' }
##' \item{\code{get_responses}}{
##'
##'   Retrieve responses to a give message id from one or more workers.
##'
##'   \emph{Usage:}
##'   \code{get_responses(message_id = , worker_ids = NULL, delete = FALSE,
##'       wait = 0)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{message_id}}{
##'       id of the message (as returned by \code{send_message}
##'     }
##'
##'     \item{\code{worker_ids}}{
##'
##'       optional vector of worker ids, or \code{NULL} (the default) to try all known workers.
##'     }
##'
##'     \item{\code{delete}}{
##'
##'       delete the response after a successful retrieval of \emph{all} responses?
##'     }
##'
##'     \item{\code{wait}}{
##'
##'       Number of seconds to wait for a response.  We poll the database repeatedly during this interval.  If 0, then a response is requested immediately.  If no response is recieved from all workers in time, an error is raised.
##'     }
##'   }
##'
##'   \emph{Value}:
##'   Always returns a list, even if only one worker id is given.
##' }
##' \item{\code{get_response}}{
##'
##'   As for \code{get_responses}, but only for a single worker id, and returns the value of the response rather than a list.
##'
##'   \emph{Usage:}
##'   \code{get_response(message_id = , worker_id = , delete = FALSE, wait = 0)}
##'
##'   \emph{Arguments:}
##'   \describe{
##'     \item{\code{message_id}}{
##'       message id
##'     }
##'
##'     \item{\code{worker_id}}{
##'       single worker id
##'     }
##'
##'     \item{\code{delete}}{
##'       delete response after successful retrieval?
##'     }
##'
##'     \item{\code{wait}}{
##'       how long to wait for a message, in seconds
##'     }
##'   }
##' }
##' \item{\code{response_ids}}{
##'
##'   Get list of message ids that a given worker has responses for.
##'
##'   \emph{Usage:}
##'   \code{response_ids(worker_id = )}
##' }
##' \item{\code{tasks_drop}}{
##'   Drop tasks from the database
##'
##'   \emph{Usage:}
##'   \code{tasks_drop(task_ids = )}
##' }
##' \item{\code{tasks_set_group}}{
##'
##'   Set the group name for one or more tasks.  The tasks can be pending, running or completed, and the tasks can already have a group ir can be groupless.  Once tasks have been grouped they can be easier to work with as a set (see \code{tasks_in_groups} and \code{task_bundle_get} in \code{\link{observer}}.
##'
##'   \emph{Usage:}
##'   \code{tasks_set_group(task_ids = , group = , exists_action = "stop")}
##' }
##' }