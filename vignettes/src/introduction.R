## ---
## title: "Introduction to rrqueue"
## author: "Rich FitzJohn"
## date: "`r Sys.Date()`"
## output: rmarkdown::html_vignette
## vignette: >
##   %\VignetteIndexEntry{Introduction to rrqueue}
##   %\VignetteEngine{knitr::rmarkdown}
##   %\VignetteEncoding{UTF-8}
## ---

## Documenting things that work asynchronously is difficult.  This
## document gives a tutorial-style overview of working with rrqueue.

## # Getting started

## The queue and workers can be started in any order, but it's easiest
## to explain starting the queue first.

## Suppose we have some simulation code; it needs to be in a file that
## the queue can see.  For now, I'll use the file `myfuns.R` which is
## the test code.  It has a function in it called `slowdouble` that
## takes a number, sleeps for that many seconds, and then returns
## twice the number.  It's useful for testing.

##+ echo=FALSE, results="hide"
rrqueue:::queue_clean(RedisAPI::hiredis(), "myqueue",
                      purge=TRUE, stop_workers=TRUE,
                      kill_local_workers=TRUE, wait_stop=0.1)
lang_output <- function(x, lang) {
  cat(c(sprintf("```%s", lang), x, "```"), sep="\n")
}
cpp_output <- function(x) lang_output(x, "c++")
r_output <- function(x) lang_output(x, "r")
yaml_output <- function(x) lang_output(x, "yaml")
plain_output <- function(x) lang_output(x, "plain")

## You'll also need a running Redis server.  I have one operating with
## the default parameters, so this works:
RedisAPI::hiredis()$PING()

## Create queue called "myqueue", tell it to load the source file
## "myfuns.R".  If it was to load packages, then passing
## `packages=c("package1", "package2")` would indicate that workers
## would need to load those packages, too.
obj <- rrqueue::queue("myqueue", sources="myfuns.R")

## The message "creating new queue" here indicates that `rrqueue` did
## not find any previous queues in place.  Queues are designed to be
## re-attachable so we can immediately just do that:
obj <- rrqueue::queue("myqueue", sources="myfuns.R")

## The message also notes that we have no workers available, so no
## work is going to get done.  But we can still queue some tasks.

## # Queuing tasks

## The simplest sort of task queuing is to pass an expression into enqueue:
t <- obj$enqueue(1 + 1)

## The expression is not evaluated but stored and will be evaluated on
## the worker.  Saving the result of this gives a `task` object which
## can be inspected.
t

## The expression stored in the task:
t$expr()

## The status of the task:
t$status()

## The result of the task, which will throw an error if we try to call it:
##+ error=TRUE
t$result()

## And how long the task has been waiting:
t$times()

## Tasks can use local variables, too:
x <- 10
t2 <- obj$enqueue(x * 2)
t2$expr()

## And because using unevaluated expressions can be problematic,
## `rrqueue` has a standard-evaluation version (`enqueue_`) which takes
## either strings representing expressions or quoted expressions:
### obj$enqueue_("x / 2")
obj$enqueue_(quote(x / 2))

## Now we have three tasks:
obj$tasks_list()

## All the tasks are waiting to be run:
obj$tasks_status()

## We can get an overview of the tasks:
obj$tasks_overview()

## # Starting workers

## `rrqueue` includes a script `rrqueue_worker` for starting workers
## from the command line (install with `rrqueue::install_scripts()`.
## Workers can also be started from within R using the `worker_spawn`
## function:
logfile <- tempfile()
wid <- rrqueue::worker_spawn("myqueue", logfile)
##+ echo=FALSE
Sys.sleep(.5)

## This function returns the \emph{worker identifier}, which is also
## printed to the screen.

## It's probably informative at this point to read the logfile of the
## worker to see what it did on startup:

### See the RcppR6 tutorial for how to do this nicely.
##+ results="asis", echo=FALSE
plain_output(readLines(logfile))

## The worker first prints a lot of diagnostic information to the
## screen (or log file) indicating the name of the worker, the version
## of rrqueue, machine information, and special keys in the database
## where important information is stored.

## Then after broadcasting that it is awake (`ALIVE`) it detected that
## there was a controller on the queue and it attempts to construct
## the environment that the controller wants `r paste("ENVIR", obj$envir_id)`.

## After that, there are a series of `TASK_START`, `EXPR`, and
## `TASK_COMPLETE` lines as each of the three tasks is processed.
obj$tasks_status()

## The times here give an indication of the rrqueue overhead; the
## running time of these simple expressions should be close to zero.
obj$tasks_times()

## The task handle created before can now give a result:
t$result()

## Similarly, results can be retrieved from the queue directly:
obj$task_result(1)
obj$task_result(2)
obj$task_result(3)

## The worker that we created can be seen here:
obj$workers_list()

## Queue a slower task; this time the `slowdouble` function.  This
## will take 1s:
t <- obj$enqueue(slowdouble(1))
t$status()
Sys.sleep(.3)
t$status()
Sys.sleep(1)
t$status()
t$result()

## Again, times are available:
t$times()

## # Finishing up

## Messaging will be properly dealt with in another vignette.
id <- obj$send_message("STOP")
obj$get_response(id, wid, wait=10)

##+ results="asis", echo=FALSE
plain_output(readLines(logfile))

## worker is now in the exited list
obj$workers_list_exited()

## The full log from our worker (dropping the first column which is
## the worker id and takes up valuble space here):
obj$workers_log_tail(wid, Inf)[-1]
