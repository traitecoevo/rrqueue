## ---
## title: "rrqueue messages"
## author: "Rich FitzJohn"
## date: "`r Sys.Date()`"
## output: rmarkdown::html_vignette
## vignette: >
##   %\VignetteIndexEntry{rrqueue messages}
##   %\VignetteEngine{knitr::rmarkdown}
##   %\VignetteEncoding{UTF-8}
## ---

##+ echo=FALSE, results="hide"
rrqueue:::queue_clean(RedisAPI::hiredis(), "myqueue",
                      purge=TRUE, stop_workers=TRUE,
                      kill_local=TRUE, wait_stop=0.1)
lang_output <- function(x, lang) {
  cat(c(sprintf("```%s", lang), x, "```"), sep="\n")
}
make_reader <- function(filename, lang="plain") {
  n <- 0
  force(filename)
  force(lang)
  function() {
    txt <- readLines(logfile)
    if (n > 0) {
      txt <- txt[-seq_len(n)]
    }
    n <<- n + length(txt)
    lang_output(txt, lang)
  }
}

## In addition to passing tasks (and results) between a controller and
## workers, the controller can also send "messages" to workers.  This
## vignette shows what the possible messages do.

## In order to do this, we're going to need a queue and a worker:
obj <- rrqueue::queue("myqueue", sources="myfuns.R")
logfile <- tempfile()
worker_id <- rrqueue::worker_spawn("myqueue", logfile)

## On startup the worker log contains:
##+ results="asis", echo=FALSE
reader <- make_reader(logfile)
reader()

## Because one of the main effects of messages is to print to the
## worker logfile, we'll print this fairly often.

## ## Messages and responses

## 1. The queue sends a message for one or more workers to process.
##    The message has an *identifier* that is derived from the current
##    time.  Messages are written to a first-in-first-out queue, *per
##    worker*, and are processed independently by workers who do not
##    look to see if other workers have messages or are processing
##    them.
##
## 2. As soon as a worker has finished processing any current job it
##    will process the message (it must wait to finish a current job
##    but will not start any further jobs).
##
## 3. Once the message has been processed (see below) a response will
##    be written to a response list with the same identifier as the
##    message.

## ## `PING`

## The `PING` message simply asks the worker to return `PONG`.  It's
## useful for diagnosing communication issues because it does so
## little
message_id <- obj$send_message("PING")

## The message id is going to be useful for getting responses:
message_id

## (this is derived from the current time, according to Redis which is
## the central reference point of time for the whole system).

## wait a little while:
Sys.sleep(.5)

##+ results="asis", echo=TRUE
reader()

## The logfile prints:

## 1. the request for the `PING` (`MESSAGE PING`)
## 2. the value `PONG` to the R message stream
## 3. logging a response (`RESPONSE PONG`), which means that something is written to the response stream.

## We can access the same bits of information in the worker log:
obj$workers_log_tail(n=Inf)[-1]

## This includes the `ALIVE` and `ENVIR` bits as the worker comes up.

## Inspecting the logs is fine for interactive use, but it's going to
## be more useful often to poll for a response.

## We already know that our worker has a response, but we can ask anyway:
obj$has_responses(message_id)

## Or inversely we can as what messages a given worker has responses for:
obj$response_ids(worker_id)

## To fetch the responses from all workers it was sent to (always
## returning a named list):
obj$get_responses(message_id)

## or to fetch the response from a given worker:
obj$get_response(message_id, worker_id)

## The response can be deleted by passing `delete=TRUE` to this method:
obj$get_response(message_id, worker_id, delete=TRUE)

## after which recalling the message will throw an error:
##+ error=TRUE
obj$get_response(message_id, worker_id, delete=TRUE)

## There is also a `wait` argument that lets you wait until a response
## is ready.  The `slowdouble` command will take a few seconds, so to
## demonstrate:
obj$enqueue(slowdouble(2))
message_id <- obj$send_message("PING")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## Looking at the log will show what went on here:
obj$workers_log_tail(n=4)[-1]

## 1. A task is recieved
## 2. 2s later the task is completed
## 3. Then the message is recieved
## 4. Then, basically instantaneously, the message is responded to

## However, because the message is only processed after the task is
## completed, the response takes a while to come back.  Equivalently,
## from the worker log:

##+ results="asis", echo=FALSE
reader()

## ## `ECHO`

## This is basically like `PING` and not very interesting; it prints
## an arbitrary string to the log.  It always returns `"OK"` as a
## response.

message_id <- obj$send_message("ECHO", "hello world!")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

##+ results="asis", echo=FALSE
reader()

## ## `INFO`

## The `INFO` command refreshes and returns the worker information.

## We already have a copy of the worker info; it was created when the
## worker started up:
obj$workers_info()[[worker_id]]

## Note that the `envir` field is currently empty (`{}`) because when
## the worker started it did not know about any environments.

message_id <- obj$send_message("INFO")

## Here's the new worker information, complete with an updated `envir`
## field:
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## This has been updated on the database copy too:
obj$workers_info()[[worker_id]]$envir

## and the same information is printed to the worker log:
##+ results="asis", echo=FALSE
reader()

## ## `DIR`

## This is useful for listing directory contents, similar to the `dir`
## function in R.  However, because file *contents* are usually more
## interesting (e.g., working out why something is not running on the
## remote machine), this is basically the result of passing the
## results of `dir` to `tools::md5sum` in order to get the md5sum of
## the file.
message_id <- obj$send_message("DIR")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## Additional arguments to `dir` can be passed through:
message_id <- obj$send_message("DIR", list(pattern="\\.R$"))
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## If you pass in invalid arguments to `dir`, then a reasonably
## helpful message should be generated:
message_id <- obj$send_message("DIR", list(foo="bar"))
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## (note that this does not generate an error locally, but you can
## test to see if it did throw an error by checking the class of the
## returned value).

## and the same information is printed to the worker log:
##+ results="asis", echo=FALSE
reader()

## ## `PUSH`

## The commands `PUSH` and `PULL` move files from and to the worker.
## The command is interpreted as an instruction to the worker so
## `PUSH` pushes files from the worker into the database while `PULL`
## pulls files from the database into the worker.  There are (will be)
## prefereable higher-level ways of dealing with this.

## Things to be aware of here: Redis is an in memory store and rrqueue
## is not at all agressive about deleting objects.  If you push a 1GB
## file into Redis things *will* go badly.  There are no checks for
## this at present!

## `PUSH` takes a vector of filename as an argument.  The response is
## not the file itself (how could it do that?) but instead the *hash*
## of that file.  By the time the response is recieved the file
## contents are stored in the database and can be returned.
message_id <- obj$send_message("PUSH", "myfuns.R")
res <- obj$get_response(message_id, worker_id, delete=TRUE, wait=10)
res

## We can save the file onto a temporary directory in the filesystem
## using the \code{files_unpack} method of \code{queue}:
path <- obj$files_unpack(res)
dir(path)

## And the files have the expected hash:
tools::md5sum(file.path(path, names(res)))

##+ results="asis", echo=FALSE
reader()

## ## `PULL`

## This is the inverse of `PUSH` and takes files from the machine the
## queue is running on and copies them into the worker (from the view
## of the worker, the files in question are already in the database
## and it will "pull" them down locally.

## First, we need to save files into the database.  Let's rename the
## temporary file above and save that:
file.rename(file.path(path, "myfuns.R"),
            "brandnewcode.R")
res <- obj$files_pack("brandnewcode.R")
res

## Note that the hash here is the same as above: `rrqueue` can tell
## this is the same file even though it has the same filename.  Note
## also that filenames will be interepted relative to the working
## directory, because the directory layout on the worker outside of
## this point could be arbitrarily different.

## Now the the files have been packed, we can run the PULL command:

## (note that the `PULL` command *always* unpacks files into the
## workers working directory).
message_id <- obj$send_message("PULL")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## And the new file will be present in the directory:
message_id <- obj$send_message("DIR", list(pattern="\\.R$"))
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

##+ results="asis", echo=FALSE
reader()

## ## `EVAL`

## Evaluate an arbitrary R expression, passed as a string (*not* as
## any sort of unevaluated or quoted expression).  This expression is
## evaluated in the global environment, which is *not* the environment
## in which queued code is evaluated in.

message_id <- obj$send_message("EVAL", "1 + 1")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

## We can delete the file created above:
message_id <- obj$send_message("EVAL", "file.remove('brandnewcode.R')")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

##+ results="asis", echo=FALSE
reader()

## This could be used to evaluate code that has side effects, such as
## installing packages.  However, due to limitations with how R loads
## packages the only way to update and reload a package is going to be
## to restart the worker.

## ## `STOP`

## Stop sends a shutdown message to the worker.  Generally you should
## prefer the `stop_workers` method, which uses `STOP` behind the
## scenes.

message_id <- obj$send_message("STOP")
obj$get_response(message_id, worker_id, delete=TRUE, wait=10)

##+ results="asis", echo=FALSE
reader()
