WORKER_IDLE <- 0L
WORKER_BUSY <- 1L

##' @importFrom R6 R6Class
.R6_worker <- R6::R6Class(
  "worker",

  public=list(
    con=NULL,
    queue_name=NULL,
    keys=NULL,
    env=NULL,
    timeout=NULL,
    name=NULL,
    objects=NULL,

    initialize=function(queue_name, con, timeout) {
      ## TODO: Refactor this into bits that set variable, bits that
      ## create the database and bits that start the main loop...
      banner(sprintf("_- %s! -_", .packageName))
      self$con <- redis_connection(con)
      self$timeout <- timeout

      hostname <- Sys.info()[["nodename"]]
      pid      <- Sys.getpid()

      self$name <- sprintf("%s::%d", hostname, pid)
      self$queue_name <- queue_name

      self$keys <- rrqueue_keys(self$queue_name)
      self$keys$worker_message <- rrqueue_key_worker(self$queue_name, self$name)

      self$objects <- object_cache(con, self$keys$objects)

      ## Register the worker:
      if (self$con$SISMEMBER(self$keys$workers, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }
      message("worker:   ", self$name)
      message("hostname: ", hostname)
      message("pid:      ", pid)
      redis_multi(self$con, {
        self$con$SADD(self$keys$workers, self$name)
        self$con$HSET(self$keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(self$keys$workers_task, self$name)
      })

      ## TODO: first interrupt should be to kill currently evaluating
      ## process, not full shutdown.
      catch_interrupt <- function(e) {
        ## TODO: this should only matter when running interactively,
        ## but it does apply here.  Make this ignorable.
        message("Catching interrupt - halting worker")
        self$shutdown()
      }
      catch_error <- function(e) {
        message("Caught fatal error - halting")
        self$shutdown()
        stop(e)
      }
      catch_stop_worker <- function(e) {
        message(e$message)
        self$shutdown()
      }

      tryCatch(self$initialize_environment(),
               error=catch_error)
      tryCatch(self$main(),
               StopWorker=catch_stop_worker,
               error=catch_error,
               interrupt=catch_interrupt)
    },

    ## TODO: Do this within a that waits?
    initialize_environment=function() {
      if (is.null(self$env)) {
        keys <- self$keys
        if (self$con$EXISTS(keys$packages)) {
          message("initializing environment")
          packages <- string_to_object(self$con$GET(keys$packages))
          sources <- string_to_object(self$con$GET(keys$sources))
          self$env <- create_environment(packages, sources)
          message(sprintf("\tdone (%d packages, %d sources)",
                          length(packages), length(sources)))
        }
      }
      !is.null(self$env)
    },

    ## TODO: Store time since last job.
    main=function() {
      message("waiting for jobs")
      ## TODO: should not use BLPOP here but instead use BRPOPLPUSH
      ## see http://redis.io/commands/{blpop,rpoplpush,brpoplpush}
      con <- self$con
      timeout <- self$timeout
      key_queue_jobs <- self$keys$tasks_id
      key_queue_msg  <- self$keys$worker_message
      key_queue <- c(key_queue_jobs, key_queue_msg)

      wait <- worker_wait_symbols()

      ## TODO: should be possible to send SIGHUP or something to
      ## trigger stopping current job but keep listening.
      ##
      ## TODO: another option is to look at a redis key after
      ## interrupt?
      repeat {
        task <- con$context$run(c("BLPOP", key_queue, timeout))
        if (is.null(task)) {
          message(wait())
          ## Detect the "main" key perhaps?
          ## Perhaps here just rip this out of the set?
          if (FALSE) {# (con$EXISTS(queue_counter) != 1L) {
            self$shutdown()
            break
          }
        } else {
          channel <- task[[1]]
          if (channel == key_queue_jobs) {
            self$run_job(task[[2]])
          } else { # (channel == key_queue_msg)
            self$run_message(task[[2]])
          } # no else clause...
        }
      }
    },

    ## TODO: Store job begin/end times?
    run_job=function(id) {
      keys <- self$keys
      con <- self$con

      if (!self$initialize_environment()) {
        ## This should be handled carefully; probably the best bet is
        ## to flag upstream (send a message to the controller) and
        ## push the job back to the front of the queue.
        ##
        ## However, there is not really a good case where the job
        ## queue is established but the environment bits are not.
        stop("asdfa")
      }

      expr_stored <- con$HGET(keys$tasks, id)
      if (is.null(expr_stored)) {
        ## TODO: Fail nicely here by marking the job lost and returning?
        stop("job not found")
      }
      expr <- restore_expression(expr_stored)
      message("Running job ", id)
      message("\t", deparse(expr))

      redis_multi(con, {
        con$HSET(keys$workers_status, self$name, WORKER_BUSY)
        con$HSET(keys$workers_task, self$name, id)
        con$HSET(keys$tasks_status, id, TASK_RUNNING)
      })

      env <- worker_prepare_environment(expr, self$objects,
                                        new.env(parent=self$env))
      res <- try(eval(expr, env))

      if (is_error(res)) {
        message(sprintf("\tjob %s failed", id))
      }

      redis_multi(con, {
        con$HSET(keys$tasks_result, id, object_to_string(res))
        con$HDEL(keys$workers_task, self$name)
        con$HSET(keys$tasks_status, id, TASK_COMPLETE)
        con$HSET(keys$workers_status, self$name, WORKER_IDLE)
      })

      message(sprintf("job %s complete", id))
    },

    run_message=function(msg) {
      re <- "^([^\\s]+)\\s*(.*)$"
      cmd <- sub(re, "\\1", msg, perl=TRUE)
      args <- sub(re, "\\2", msg, perl=TRUE)
      switch(cmd,
             PING=run_message_PING(args),
             ECHO=run_message_ECHO(args),
             EVAL=run_message_EVAL(args),
             STOP=run_message_STOP(args),
             ENV=run_message_ENV(self),
             INSTALL=run_message_INSTALL(args),
             message(sprintf("Recieved unknown message: [%s] [%s]", cmd, args)))
    },

    ## TODO: organise doing this on finalisation:
    shutdown=function() {
      ## TODO: declare running jobs abandoned so that they get
      ## rescheduled.
      message("shutting down")
      self$con$SREM(self$keys$workers, self$name)
      self$con$HDEL(self$keys$workers_status, self$name)
    }))

##' Create an rrqueue worker.  This blocks the main loop.
##' @title Create an rrqueue worker
##' @param queue_name Queue name
##' @param con Connection to a redis database
##' @param timeout Timeout for the blocking connection
##' @export
worker <- function(queue_name, con=NULL, timeout=5) {
  .R6_worker$new(queue_name, con, timeout)
}

worker_prepare_environment <- function(expr, object_cache, env) {
  args <- as.list(expr[-1])
  is_symbol <- vapply(args, is.symbol, logical(1))
  if (any(is_symbol)) {
    for (name in args[is_symbol]) {
      assign(name, object_cache$get(as.character(name)), env)
    }
  }
  env
}

##' @importFrom remoji emoji
worker_wait_symbols <- function() {
  if (is_terminal()) {
    prefix <- remoji::emoji(c("sleeping", "zzz", "computer"), pad=TRUE)
    clocks <- remoji::emoji(sprintf("clock%d", 1:12))
    sym <- paste0(paste(prefix, collapse=""), clocks)
  } else {
    sym <- c("-", "\\", "|", "/")
  }
  i <- 0L
  n <- length(sym)
  function() {
    if (i >= n) {
      i <<- 1
    } else {
      i <<- i + 1L
    }
    sym[[i]]
  }
}

## The message passing is really simple minded; it doesn't do
## bidirectional messaging at all yet because that's hard to get right
## from the controller.
##
## Eventually that would be something that would be useful, but it'll
## get another name I think.
run_message_PING <- function(args) {
  message("PONG")
}
run_message_ECHO <- function(args) {
  message(args)
}
run_message_EVAL <- function(args) {
  message("> EVAL ", args)
  try(eval(parse(text=args), .GlobalEnv))
}
run_message_STOP <- function(args) {
  if (args == "") {
    args <- "STOP requested by controller"
  }
  stop(StopWorker(args))
}
run_message_ENV <- function(self) {
  message("> ENV")
  self$env <- NULL
  self$initialize_environment()
}

## TODO: I have nice code code for doing this in remake I could reuse
## here
## TODO: setdiff on missing packages?
run_message_INSTALL <- function(args) {
  message("> INSTALL ", args)
  try({
    pkgs <- strsplit(args, "\\s+")[[1]]
    ## Interpret things that contain "/" as gh packages
    is_gh <- grepl("/", pkgs, fixed=TRUE)
    is_cran <- !is_gh
    if (any(is_cran)) {
      install.packages(pkgs[is_cran])
    }
    if (any(is_gh)) {
      devtools::install_github(pkgs[is_gh])
    }
  })
}

rrqueue_worker_main_options <- function(...) {
  'Usage: rrqueue_worker <queue_name>' -> doc
  oo <- options(warnPartialMatchArgs=FALSE)
  if (isTRUE(oo$warnPartialMatchArgs)) {
    on.exit(options(oo))
  }
  docopt::docopt(doc, ...)
}

rrqueue_worker_main <- function(args=commandArgs(TRUE)) {
  opts <- rrqueue_worker_main_options(args)
  worker(opts$queue_name)
}

## Should provide a controller here perhaps?
rrqueue_worker_spawn <- function(queue_name, logfile, con=NULL,
                                 timeout=5, time_poll=0.1) {
  ## I'd *really* like to get the name of the spawned process here,
  ## but I don't see how to do that.  Probably this should not really
  ## be used that much?
  ##
  ## One option is to parse the logfile for the name there.
  rrqueue_worker <- Sys.which("rrqueue_worker")
  if (rrqueue_worker == "") {
    tmp <- tempfile()
    dir.create(tmp)
    install_rrqueue_worker(tmp)
    rrqueue_worker <- file.path(tmp, "rrqueue_worker")
  }
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  ## TODO: this needs checking if the connection is nontrivial.
  ##
  ## Get the worker keys (can do more easily if we have a controlling
  ## object here).
  con <- redis_connection(con)
  keys <- rrqueue_keys(queue_name)
  key <- keys$workers
  workers <- function() {
    as.character(con$SMEMBERS(key))
  }
  workers0 <- workers()

  system2(rrqueue_worker, queue_name,
          env=env, wait=FALSE,
          stdout=logfile, stderr=logfile)

  ## This is going to be much nicer to do with proper subscriptions
  ## or something.  It should also be possible to run until some
  ## number of new workers have been spun up.
  for (i in seq_len(ceiling(timeout / time_poll))) {
    workers1 <- workers()
    if (!identical(workers1, workers0)) {
      worker_new <- setdiff(workers1, workers0)
      if (length(worker_new) == 1L) {
        return(worker_new)
      }
      stop("I am confused")
    }
    Sys.sleep(time_poll)
  }
  stop("Worker not identified in time")
}

## Copied from remake
install_rrqueue_worker <- function(destination_directory, overwrite=FALSE) {
  code <- c("#!/usr/bin/env Rscript",
            "library(methods)",
            "w <- rrqueue:::rrqueue_worker_main()")
  dest <- file.path(destimation_directory, "rrqueue_worker")
  install_script(code, dest, overwrite)
}
