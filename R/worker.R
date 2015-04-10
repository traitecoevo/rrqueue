WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"

##' @importFrom R6 R6Class
.R6_worker <- R6::R6Class(
  "worker",

  public=list(
    con=NULL,
    queue_name=NULL,
    keys=NULL,
    envir=NULL,
    timeout=NULL,
    name=NULL,
    objects=NULL,

    initialize=function(queue_name, con, timeout) {
      ## TODO: Refactor this into bits that set variable, bits that
      ## create the database and bits that start the main loop...
      banner(sprintf("_- %s! -_", .packageName))
      self$con <- redis_connection(con)
      self$timeout <- timeout
      self$envir <- list()

      hostname <- Sys.info()[["nodename"]]
      pid      <- Sys.getpid()

      self$name <- sprintf("%s::%d", hostname, pid)
      self$queue_name <- queue_name
      self$keys <- rrqueue_keys(self$queue_name, worker_name=self$name)

      self$objects <- object_cache(con, self$keys$objects)

      ## Register the worker:
      if (self$con$SISMEMBER(self$keys$workers_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }
      message("worker:   ", self$name)
      message("hostname: ", hostname)
      message("pid:      ", pid)
      redis_multi(self$con, {
        self$con$SADD(self$keys$workers_name,   self$name)
        self$con$HSET(self$keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(self$keys$workers_task,   self$name)
        self$con$DEL(self$keys$log)
        self$log("ALIVE")
        ## This announces that we're up, in case anyone cares.
        self$con$RPUSH(self$keys$workers_new,   self$name)
      })

      ## TODO: first interrupt should be to kill currently evaluating
      ## process, not full shutdown.
      catch_interrupt <- function(e) {
        ## TODO: this should only matter when running interactively,
        ## but it does apply here.  Make this ignorable.
        message("Catching interrupt - halting worker")
        self$shutdown("INTERRUPT")
      }
      catch_error <- function(e) {
        message("Caught fatal error - halting")
        self$shutdown("ERROR")
        stop(e)
      }
      catch_stop_worker <- function(e) {
        message(e$message)
        self$shutdown("OK")
      }

      tryCatch(self$main(),
               StopWorker=catch_stop_worker,
               error=catch_error,
               interrupt=catch_interrupt)
    },

    initialize_environment=function(id) {
      ## TODO: consider locking the environment
      e <- self$envir[[id]]
      if (is.null(e)) {
        message("initializing environment ", id)
        self$log("ENV", id)
        keys <- self$keys
        s2o <- string_to_object
        packages <- s2o(self$con$HGET(self$keys$envirs_packages, id))
        sources  <- s2o(self$con$HGET(self$keys$envirs_sources,  id))
        e <- create_environment(packages, sources)
        self$envir[[id]] <- e
        message(sprintf("\tdone (%d packages, %d sources)",
                        length(packages), length(sources)))
      }
      e
    },

    log=function(label, message=NULL) {
      t <- as.integer(Sys.time()) # to nearest second
      if (is.null(message)) {
        msg <- sprintf("%d %s", t, label)
      } else {
        msg <- sprintf("%d %s %s", t, label, message)
      }
      self$con$RPUSH(self$keys$log, msg)
    },

    ## TODO: Store time since last task.
    main=function() {
      message("waiting for tasks")
      ## TODO: should not use BLPOP here but instead use BRPOPLPUSH
      ## see http://redis.io/commands/{blpop,rpoplpush,brpoplpush}
      con <- self$con
      timeout <- self$timeout
      key_queue_tasks <- self$keys$tasks_id
      key_queue_msg  <- self$keys$message
      key_queue <- c(key_queue_tasks, key_queue_msg)

      wait <- worker_wait_symbols()

      ## TODO: should be possible to send SIGHUP or something to
      ## trigger stopping current task but keep listening.
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
          if (channel == key_queue_tasks) {
            self$run_task(task[[2]])
          } else { # (channel == key_queue_msg)
            self$run_message(task[[2]])
          } # no else clause...
        }
      }
    },

    ## TODO: Store task begin/end times?
    ## TODO: Push events to a log on redis: worker:log - it can be a
    ## list.  Events would be:
    ##   <TIME(int)> <COMMAND> [info]
    ##   <time> ALIVE
    ##   <time> ENV id
    ##   <time> MESSAGE content
    ##   <time> TASK_START id
    ##   <time> TASK_ERROR id
    ##   <time> TASK_COMPLETE id
    ##   <time> STOP

    ## TODO: push this out into its own function so the class is
    ## easier to follow.
    run_task=function(id) {
      keys <- self$keys
      con <- self$con

      message("Running task ", id)
      self$log("TASK_START", id)

      expr_stored <- con$HGET(keys$tasks_expr, id)
      if (is.null(expr_stored)) {
        ## TODO: Fail nicely here by marking the task lost and returning?
        stop("task not found")
      }

      envir_id <- con$HGET(keys$tasks_envir, id)
      envir_parent <- self$initialize_environment(envir_id)
      envir <- new.env(parent=envir_parent)
      expr <- restore_expression(expr_stored, envir, self$objects)
      message("\t", deparse(expr))

      redis_multi(con, {
        con$HSET(keys$workers_status, self$name, WORKER_BUSY)
        con$HSET(keys$workers_task, self$name, id)
        con$HSET(keys$tasks_status, id, TASK_RUNNING)
      })

      ## Name of the queue that we push completeness to.
      key_complete <- con$HGET(keys$tasks_complete, id)

      ## NOTE: if the underlying process *wants* to return an error
      ## this is going to be a false alarm.
      res <- try(eval(expr, envir))
      if (is_error(res)) {
        self$log("TASK_ERROR", id)
        message(sprintf("\ttask %s failed", id))
        task_status <- TASK_ERRORED
      } else {
        task_status <- TASK_COMPLETE
      }

      redis_multi(con, {
        con$HSET(keys$tasks_result, id, object_to_string(res))
        con$HDEL(keys$workers_task, self$name)
        con$HSET(keys$tasks_status, id, task_status)
        con$HSET(keys$workers_status, self$name, WORKER_IDLE)
        ## This advertises to the controller that we're done
        con$RPUSH(key_complete, id)
        self$log("TASK_COMPLETE", id)
      })

      message(sprintf("task %s complete", id))
    },

    run_message=function(msg) {
      self$log("MESSAGE", msg)
      re <- "^([^\\s]+)\\s*(.*)$"
      cmd <- sub(re, "\\1", msg, perl=TRUE)
      args <- sub(re, "\\2", msg, perl=TRUE)
      ## TODO: purge object cache (save on memory)
      switch(cmd,
             PING=run_message_PING(args),
             ECHO=run_message_ECHO(args),
             EVAL=run_message_EVAL(args),
             STOP=run_message_STOP(args),
             INSTALL=run_message_INSTALL(args),
             message(sprintf("Recieved unknown message: [%s] [%s]", cmd, args)))
    },

    ## TODO: organise doing this on finalisation:
    shutdown=function(status="OK") {
      ## TODO: declare running tasks abandoned so that they get
      ## rescheduled.
      message("shutting down")
      self$con$SREM(self$keys$workers_name,   self$name)
      self$con$HDEL(self$keys$workers_status, self$name)
      self$log("STOP", status)
    }))

##' Create an rrqueue worker.  This blocks the main loop.
##' @title Create an rrqueue worker
##' @param queue_name Queue name
##' @param con Connection to a redis database
##' @param timeout Timeout for the blocking connection
##' @export
worker <- function(queue_name, con=NULL, timeout=60) {
  .R6_worker$new(queue_name, con, timeout)
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
##
## TODO: more work needed if the connection is nontrivial; the worker
## will be spawned to look at a trivial connection, until I fix the
## docopt script to allow other options (host/port/pw).  I don't think
## I can get that easily from RcppRedis::Redis though.
rrqueue_worker_spawn <- function(queue_name, logfile,
                                 timeout=10, time_poll=1) {
  rrqueue_worker <- rrqueue_worker_script()
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  con <- redis_connection(NULL)
  key_workers_new <- rrqueue_keys(queue_name)$workers_new

  ## Sanitity check:
  if (con$LLEN(key_workers_new) > 0L) {
    stop("Clear the new workers list first: ", key_workers_new)
  }

  code <- system2(rrqueue_worker, queue_name,
                  env=env, wait=FALSE,
                  stdout=logfile, stderr=logfile)
  if (code != 0L) {
    warning("Error launching script: worker *probably* does not exist")
  }

  time_poll <- 1
  timeout <- 10
  for (i in seq_len(ceiling(timeout / time_poll))) {
    x <- con$context$run(c("BLPOP", key_workers_new, timeout))
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
