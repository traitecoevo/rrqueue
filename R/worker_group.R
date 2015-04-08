## Make a group of n workers.
## TODO: What happens if one of these gets taken out?  We should mark
## it dead in the status I guess.  But then this doesn't actually do
## very much with the processes apart from start them.
.R6_worker_group <- R6::R6Class(
  "worker_group",

  public=list(
    queue_name=NULL,
    workers=NULL,
    con=NULL,
    keys=NULL,
    poll=NULL,

    initialize=function(queue_name, n, logfile_fmt, poll, con) {
      banner(sprintf("_- %s! -_", .packageName))
      self$queue_name <- queue_name
      self$con <- redis_connection(con)
      self$keys <- rrqueue_keys(queue_name)
      self$poll <- poll

      if (n < 1) {
        stop("n must be at least 1")
      }

      logfile <- sprintf(logfile_fmt, seq_len(n))

      ## TODO: cleanup if anything fails here.
      id <- character(n)
      for (i in seq_len(n)) {
        id[[i]] <- rrqueue_worker_spawn(queue_name, logfile[[i]])
      }

      self$workers <- id
      self$keys$workers <- rrqueue_key_worker(self$queue_name, self$workers)

      message("Workers:\n", paste0("\t", self$workers, collapse="\n"))

      catch <- function(e) {
        message("Shutting down")
        self$shutdown()
        stop(e)
      }
      tryCatch(self$main(), error=catch)
    },

    main=function() {
      t <- self$poll
      if (is_terminal()) {
        sym <- remoji::emoji(c("x", "computer"), pad=TRUE)
      } else {
        sym <- c(".", "*")
      }
      names(sym) <- c(WORKER_IDLE, WORKER_BUSY)
      repeat {
        ## TODO: when a worker dies, this will say NA in the status --
        ## that's actually pretty good really.
        status <- as.character(self$con$HMGET(self$keys$workers_status,
                                              self$workers))
        message(sprintf("[ %s ] %s", Sys.time(),
                        paste(sym[status], collapse=" ")))
        Sys.sleep(t)
      }
    },

    shutdown=function() {
      ## It would be better to attach the controller here, but that
      ## requires that the "observe only" functionality is working.
      for (k in self$keys$workers) {
        self$con$RPUSH(k, "STOP")
      }
      ## TODO: Wait, check pids to see which are alive, send kill
      ## signals if not.
    }
  ))

##' Create group of rrqueue workers
##' @title Create group of rrqueue workers
##' @param queue_name Queue name
##' @param n Number of processes to start
##' @param logfile_fmt Format for the log file, containing a
##' percentage-d for \code{sprintf}
##' @param poll Frequency, in seconds, to poll for updates
##' @param con Redis connection.  If this is not NULL probably nothing
##' good will happen.
##' @export
worker_group <- function(queue_name, n, logfile_fmt, poll=10, con=NULL) {
  .R6_worker_group$new(queue_name, n, logfile_fmt, poll, con)
}

worker_group_main <- function(args=commandArgs(TRUE)) {
  'Usage: rrqueue_worker_group <queue_name> <n> <logfile_fmt>' -> doc
  opts <- docopt_parse(doc, args)
  worker_group(opts$queue_name, opts$n, opts$logfile_fmt)
}

install_rrqueue_worker_group <- function(destination_directory,
                                         overwrite=FALSE) {
  code <- c("#!/usr/bin/env Rscript",
            "library(methods)",
            "w <- rrqueue:::worker_group_main()")
  dest <- file.path(destination_directory, "rrqueue_worker_group")
  install_script(code, dest, overwrite)
}
