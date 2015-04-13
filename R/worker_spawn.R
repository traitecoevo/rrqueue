rrqueue_worker_main <- function(args=commandArgs(TRUE)) {
  'Usage: rrqueue_worker [options] <queue_name>

  --heartbeat-period T  Heartbeat period [default: 10]
  --heartbeat-expire T  Heartbeat expiry time [default: 30]
  ' -> doc
  oo <- options(warnPartialMatchArgs=FALSE)
  if (isTRUE(oo$warnPartialMatchArgs)) {
    on.exit(options(oo))
  }
  opts <- docopt::docopt(doc, args)
  worker(opts$queue_name)
}

## Should provide a controller here perhaps?
##
## TODO: more work needed if the connection is nontrivial; the worker
## will be spawned to look at a trivial connection, until I fix the
## docopt script to allow other options (host/port/pw).  I don't think
## I can get that easily from RcppRedis::Redis though.
rrqueue_worker_spawn <- function(queue_name, logfile,
                                 timeout=20, time_poll=3,
                                 heartbeat_period=NULL,
                                 heartbeat_expire=NULL) {
  rrqueue_worker <- rrqueue_worker_script()
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  con <- redis_connection(NULL)
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
  opts <- c(opts, queue_name)
  code <- system2(rrqueue_worker, opts,
                  env=env, wait=FALSE,
                  stdout=logfile, stderr=logfile)
  if (code != 0L) {
    warning("Error launching script: worker *probably* does not exist")
  }

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

## Copied from remake
install_rrqueue_worker <- function(destination_directory, overwrite=FALSE) {
  code <- c("#!/usr/bin/env Rscript",
            "library(methods)",
            "w <- rrqueue:::rrqueue_worker_main()")
  dest <- file.path(destination_directory, "rrqueue_worker")
  install_script(code, dest, overwrite)
}

rrqueue_worker_script <- function() {
  rrqueue_worker <- Sys.which("rrqueue_worker")
  if (rrqueue_worker == "") {
    tmp <- tempfile()
    dir.create(tmp)
    install_rrqueue_worker(tmp)
    rrqueue_worker <- file.path(tmp, "rrqueue_worker")
  }
  rrqueue_worker
}
