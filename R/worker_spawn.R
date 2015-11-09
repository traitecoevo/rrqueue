## There are *four* ways of launching workers; that's why this is so
## complicated.
##
## 1. manually using worker(...)
## 2. from R using worker_spawn(...)
## 3. from the command line using rrqueue_worker
## 4. from the command line using rrqueue_worker_tee

## TODO: autogenerate this in the same way that we do for the args
rrqueue_worker_main <- function(args=commandArgs(TRUE)) {
  opts <- rrqueue_worker_args(args)
  con <- redux::hiredis(host=opts$redis_host,
                        port=as.integer(opts$redis_port))
  worker(opts$queue_name, con,
         heartbeat_period=opts$heartbeat_period,
         heartbeat_expire=opts$heartbeat_expire,
         key_worker_alive=opts$key_worker_alive)
}

rrqueue_worker_args <- function(args=commandArgs(TRUE)) {
  'Usage:
  rrqueue_worker [options] <queue_name>
  rrqueue_worker --config=FILENAME [options] [<queue_name>]
  rrqueue_worker -h | --help

Options:
  --redis-host HOSTNAME   Hostname for Redis
  --redis-port PORT       Port for Redis
  --heartbeat-period T    Heartbeat period
  --heartbeat-expire T    Heartbeat expiry time
  --key-worker-alive KEY  Key to write to when the worker becomes alive
  --config FILENAME       Optional YAML configuration filename

  Arguments:
  <queue_name>   Name of queue
  ' -> doc

  given <- docopt_parse(doc, args)
  given <- given[!vlapply(given, is.null)]

  defaults <- as.list(formals(worker))
  nms <- names(defaults)
  defaults$queue_name <- NULL

  ## This will be an issue for docopt interface to the main queue too.
  if (is.null(given$config)) {
    ret <- modifyList(defaults, given)[nms]
  } else {
    cfg <- tmp_fix_redis_config(load_config(given$config))
    ret <- modifyList(modifyList(defaults, cfg), given)[nms]
    ## Check that we did get a queue_name
    if (is.null(ret$queue_name)) {
      stop("queue name must be given")
    }
  }
  ret
}

##' Spawn a worker in the background
##'
##' Spawning multiple workers.  If \code{n} is greater than one,
##' multiple workers will be spawned.  This happens in parallel so it
##' does not take n times longer than spawing a single worker.
##'
##' Beware that signals like Ctrl-C passed to \emph{this} R instance
##' can still propagate to the child processes and can result in them
##' dying unexpectedly.  It is probably safer to start processes in a
##' standalone session.
##'
##' @title Spawn a worker
##' @param queue_name Name of the queue to connect to
##' @param logfile Name of a log file to write to (consider
##'   \code{tempfile()}).  If \code{n} > 1, then \code{n} log files
##'   must be provided.
##' @param redis_host Host name/IP for the Redis server
##' @param redis_port Port for the Redis server
##' @param n Number of workers to spawn
##' @param timeout Time to wait for the worker to appear
##' @param time_poll Period to poll for the worker (must be in
##'   seconds)
##' @param heartbeat_period Period between heartbeat pulses
##' @param heartbeat_expire Time that heartbeat pulses will persist
##' @param path Path to start the worker in.  By default workers will
##'   start in the current working directory, but you can start them
##'   elsewhere by providing a path here.  If the path does not exist,
##'   an error will be thrown.  If \code{n} is greater than 1, all
##'   workers will start in the same working directory.  The
##'   \code{logfile} argument will be interpreted relative to current
##'   working directory (not the worker working directory); use
##'   \code{\link{normalizePath}} to convert into an absolute path
##'   name to prevent this.
##' @export
worker_spawn <- function(queue_name, logfile,
                         redis_host="127.0.0.1",
                         redis_port=6379,
                         n=1,
                         timeout=20, time_poll=1,
                         heartbeat_period=NULL,
                         heartbeat_expire=NULL,
                         path=".") {
  rrqueue_worker <- find_script("rrqueue_worker")
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  if (length(logfile) != n) {
    stop("logfile must be a vector of length n")
  }
  assert_integer_like(time_poll)

  con <- redux::hiredis(host=redis_host, port=redis_port)
  key_worker_alive <- rrqueue_key_worker_alive(queue_name)

  opts <- character(0)
  if (!is.null(heartbeat_expire)) {
    opts <- c(opts, "--heartbeat-expire", heartbeat_expire)
  }
  if (!is.null(heartbeat_period)) {
    opts <- c(opts, "--heartbeat-period", heartbeat_period)
  }
  if (!is.null(redis_host)) {
    opts <- c(opts, "--redis-host", redis_host)
  }
  if (!is.null(redis_port)) {
    opts <- c(opts, "--redis-port", redis_port)
  }
  opts <- c(opts, "--key-worker-alive", key_worker_alive, queue_name)

  dir_create(dirname(logfile))
  logfile <- file.path(normalizePath(dirname(logfile)), basename(logfile))

  code <- integer(n)
  with_wd(path, {
    for (i in seq_len(n)) {
      code[[i]] <- system2(rrqueue_worker, opts,
                           env=env, wait=FALSE,
                           stdout=logfile[[i]], stderr=logfile[[i]])
    }
  })
  if (any(code != 0L)) {
    warning("Error launching script: worker *probably* does not exist")
  }

  ret <- rep.int(NA_character_, n)
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")

  i <- 1L
  repeat {
    x <- con$BLPOP(key_worker_alive, time_poll)
    if (is.null(x)) {
      message(".", appendLF=FALSE)
      flush.console()
    } else {
      ret[[i]] <- x[[2]]
      if (n > 1L) {
        message(sprintf("new worker: %s (%d / %d)", x[[2]], i, n))
      } else {
        message(sprintf("new worker: %s", x[[2]]))
      }
      i <- i + 1
    }
    if (!any(is.na(ret))) {
      break
    }
    if (Sys.time() - t0 > timeout) {
      ## TODO: Better recover here.  Ideally we'd stop any workers
      ## that *are* running, and provide data from the log files.
      stop(sprintf("%d / %d workers not identified in time",
                   sum(is.na(ret)), n))
    }
  }

  ret
}
