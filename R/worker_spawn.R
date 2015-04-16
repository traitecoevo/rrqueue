## There are *four* ways of launching workers; that's why this is so
## complicated.
##
## 1. manually using worker(...)
## 2. from R using rrqueue_worker_spawn(...)
## 3. from the command line using rrqueue_worker
## 4. from the command line using rrqueue_worker_tee

## TODO: autogenerate this in the same way that we do for the args
## TODO: expose the redis arguments directly rather than as 'con'?
rrqueue_worker_main <- function(args=commandArgs(TRUE)) {
  'Usage:
  rrqueue_worker [options] <queue_name>
  rrqueue_worker -h | --help

  Options:
  --redis-host HOSTNAME  Hostname for Redis [default: 127.0.0.1]
  --redis-port PORT      Port for Redis [default: 6379]
  --heartbeat-period T   Heartbeat period [default: 10]
  --heartbeat-expire T   Heartbeat expiry time [default: 30]

  Arguments:
  <queue_name>   Name of queue, or omit to read from rrqueue.yml
  ' -> doc
  oo <- options(warnPartialMatchArgs=FALSE)
  if (isTRUE(oo$warnPartialMatchArgs)) {
    on.exit(options(oo))
  }
  opts <- docopt::docopt(doc, args)
  names(opts) <- sub("-", "_", names(opts))

  con <- RedisAPI::hiredis(opts$redis_host, as.integer(opts$redis_port))
  worker(opts$queue_name, con,
         heartbeat_period=opts$heartbeat_period,
         heartbeat_expire=opts$heartbeat_expire)
}

## Should provide a controller here perhaps?
##
## TODO: more work needed if the connection is nontrivial; the worker
## will be spawned to look at a trivial connection, until I fix the
## docopt script to allow other options (host/port/pw).  I don't think
## I can get that easily from RcppRedis::Redis though.
rrqueue_worker_spawn <- function(queue_name, logfile,
                                 redis_host="127.0.0.1",
                                 redis_port=6379,
                                 timeout=20, time_poll=3,
                                 heartbeat_period=NULL,
                                 heartbeat_expire=NULL) {
  rrqueue_worker <- find_script("rrqueue_worker")
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  con <- RedisAPI::hiredis(redis_host, redis_port)
  key_workers_new <- rrqueue_keys(queue_name)$workers_new
  ## Sanitity check:
  if (con$LLEN(key_workers_new) > 0L) {
    stop("Clear the new workers list first: ", key_workers_new)
  }

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
  opts <- c(opts, queue_name)
  code <- system2(rrqueue_worker, opts,
                  env=env, wait=FALSE,
                  stdout=logfile, stderr=logfile)
  if (code != 0L) {
    warning("Error launching script: worker *probably* does not exist")
  }

  ## TODO: we could monitor the file here perhaps?  Especially on error.
  for (i in seq_len(ceiling(timeout / time_poll))) {
    x <- con$BLPOP(key_workers_new, time_poll)
    if (is.null(x)) {
      message(".", appendLF=FALSE)
      flush.console()
    } else {
      new_worker <- x[[2]]
      message("new worker: ", new_worker)
      return(new_worker)
    }
  }
  stop("Worker not identified in time")
}
