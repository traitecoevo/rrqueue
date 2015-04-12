WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_LOST <- "LOST"

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
    styles=NULL,

    initialize=function(queue_name, con, timeout) {
      self$queue_name <- queue_name
      self$con <- redis_connection(con)
      self$timeout <- timeout

      self$envir <- list()
      self$styles <- worker_styles()
      self$name <- sprintf("%s::%d", Sys.info()[["nodename"]], Sys.getpid())
      self$keys <- rrqueue_keys(self$queue_name, worker_name=self$name)

      ## TODO: first interrupt should be to kill currently evaluating
      ## process, not full shutdown.
      catch_interrupt <- function(e) {
        ## TODO: this should only matter when running interactively,
        ## but it does apply here.  Make this ignorable.
        message("Catching interrupt - halting worker")
        self$shutdown("INTERRUPT")
        ## TODO: flag all jobs immediately as orphaned?
      }
      catch_error <- function(e) {
        message("Caught fatal error - halting")
        self$shutdown("ERROR")
        stop(e)
      }
      catch_worker_stop <- function(e) {
        self$shutdown("OK")
      }

      ## NOTE: This needs to happen *before* running the
      ## initialize_worker; it checks that we can actually use the
      ## database and that we will not write anything to an existing
      ## worker.  An error here will not be caught.
      ##
      ## TODO: This could just throw WorkerClash and get a different
      ## handler in the tryCatch
      if (self$con$SISMEMBER(self$keys$workers_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      tryCatch(self$initialize_worker(),
               error=catch_error)
      tryCatch(self$main(),
               WorkerStop=catch_worker_stop,
               error=catch_error,
               interrupt=catch_interrupt)
    },

    initialize_worker=function() {
      self$print_info()
      redis_multi(self$con, {
        self$con$SADD(self$keys$workers_name,   self$name)
        self$con$HSET(self$keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(self$keys$workers_task,   self$name)
        self$con$DEL(self$keys$log)
        self$con$DEL(self$keys$heartbeat)
        self$log("ALIVE")
        ## This announces that we're up, in case anyone cares.
        self$con$RPUSH(self$keys$workers_new,   self$name)
      })
      self$objects <- object_cache(self$con, self$keys$objects)
    },

    initialize_environment=function(id) {
      ## TODO: consider locking the environment
      e <- self$envir[[id]]
      if (is.null(e)) {
        self$log("ENVIR", id)
        keys <- self$keys
        dat_str <- self$con$HGET(self$keys$envirs_contents, id)
        dat <- string_to_object(dat_str)

        ## Check the hashes of the files
        ## TODO: this all needs to be run in tryCatch
        hash_expected <- dat$source_files
        if (length(hash_expected) > 0L) {
          hash_recieved <- hash_file(names(hash_expected))
          if (!identical(hash_expected, hash_recieved)) {
            stop("Files are not the same")
          }
        }

        e <- create_environment(dat$packages, dat$sources)
        self$envir[[id]] <- e
        ## Here; can do a bit better:
        fmt <- function(x) {
          if (is.null(x)) {
            "(none)"
          } else {
            paste(x, collapse=" ")
          }
        }
        self$log("ENVIR PACKAGES", fmt(dat$packages), push=FALSE)
        self$log("ENVIR SOURCES",  fmt(dat$sources),  push=FALSE)
      }
      e
    },

    ## TODO: if we're not running in a terminal, then we should output
    ## the worker id into the screen message.
    log=function(label, message=NULL, push=TRUE) {
      t <- Sys.time()
      ti <- as.integer(t) # to nearest second
      ts <- self$styles$info(as.character(t))
      if (is.null(message)) {
        msg_log <- sprintf("%d %s", ti, label)
        msg_scr <- sprintf("[%s] %s", ts, self$styles$key(label))
      } else {
        msg_log <- sprintf("%d %s %s", ti, label, message)
        msg_scr <- sprintf("[%s] %s %s", ts, self$styles$key(label),
                           self$styles$value(message))
      }
      message(msg_scr)
      if (push) {
        self$con$RPUSH(self$keys$log, msg_log)
      }
    },

    ## TODO: Store time since last task.
    main=function() {
      ## TODO: should not use BLPOP here but instead use BRPOPLPUSH
      ## see http://redis.io/commands/{blpop,rpoplpush,brpoplpush}
      con <- self$con
      timeout <- self$timeout
      key_queue_tasks <- self$keys$tasks_id
      key_queue_msg  <- self$keys$message
      key_queue <- c(key_queue_tasks, key_queue_msg)

      ## TODO: should be possible to send SIGHUP or something to
      ## trigger stopping current task but keep listening.
      ##
      ## TODO: another option is to look at a redis key after
      ## interrupt?
      repeat {
        task <- con$context$run(c("BLPOP", key_queue, timeout))
        if (is.null(task)) {
          self$log("WAITING", push=FALSE)
        } else {
          channel <- task[[1]]
          if (channel == key_queue_tasks) {
            tryCatch(
              self$run_task(task[[2]]),
              WorkerError=function(e)
                self$task_cleanup(e, e$task_id, e$task_status))
          } else { # (channel == key_queue_msg)
            self$run_message(task[[2]])
          } # no else clause...
        }
      }
    },

    run_task=function(id) {
      keys <- self$keys
      con <- self$con
      ## Need to start this as soon as possible after taking the job;
      ## ideally we'd do it as we pop the job.  If this one fails,
      ## it's all over really.
      h <- heartbeat(keys$heartbeat, 10, 30, con)
      ## This would happen with garbage collection anyway, but now
      ## happens deterministically.
      on.exit(h$stop())

      self$log("TASK_START", id)

      expr <- self$task_retrieve(id)

      context <- tryCatch(
        self$task_prepare(id, expr),
        error=function(e) stop(WorkerEnvironmentFailed(self, id, e)))

      ## Here, we get time from the Redis server, not R; that means
      ## that all ideas of time are centralised.
      time <- redis_time(con)
      redis_multi(con, {
        con$HSET(keys$workers_status, self$name, WORKER_BUSY)
        con$HSET(keys$workers_task,   self$name, id)
        con$HSET(keys$tasks_worker,   id,        self$name)
        con$HSET(keys$tasks_status,   id,        TASK_RUNNING)
        con$HSET(keys$tasks_time_0,   id,        time)
      })

      ## NOTE: if the underlying process *wants* to return an error
      ## this is going to be a false alarm, but there's not really a
      ## good way of detecting that.
      res <- try(eval(context$expr, context$envir))
      status <- if (is_error(res)) TASK_ERROR else TASK_COMPLETE
      self$task_cleanup(res, id, status)
    },

    run_message=function(msg) {
      self$log("MESSAGE", msg)
      re <- "^([^\\s]+)\\s*(.*)$"
      cmd <- sub(re, "\\1", msg, perl=TRUE)
      args <- sub(re, "\\2", msg, perl=TRUE)
      ## TODO: purge object cache (save on memory)
      switch(cmd,
             PING=message("PONG"),
             ECHO=message(args),
             EVAL=run_message_EVAL(args),
             STOP=run_message_STOP(self, args),
             INFO=self$print_info(),
             INSTALL=run_message_INSTALL(args),
             message(sprintf("Recieved unknown message: [%s] [%s]", cmd, args)))
    },

    task_retrieve=function(id) {
      expr_stored <- self$con$HGET(self$keys$tasks_expr, id)
      if (is.null(expr_stored)) {
        stop(WorkerTaskMissing(self, id))
      }
      expr_stored
    },

    task_prepare=function(id, expr_stored) {
      con <- self$con
      keys <- self$keys

      envir_id <- con$HGET(keys$tasks_envir, id)
      envir_parent <- self$initialize_environment(envir_id)

      envir <- new.env(parent=envir_parent)
      expr <- restore_expression(expr_stored, envir, self$objects)
      list(expr=expr, envir=envir)
    },

    task_cleanup=function(data, id, task_status) {
      con <- self$con
      keys <- self$keys
      key_complete <- con$HGET(keys$tasks_complete, id)
      time <- redis_time(con)
      redis_multi(con, {
        con$HSET(keys$tasks_result,   id,        object_to_string(data))
        con$HSET(keys$tasks_status,   id,        task_status)
        con$HSET(keys$tasks_time_1,   id,        time)
        con$HSET(keys$workers_status, self$name, WORKER_IDLE)
        con$HDEL(keys$workers_task,   self$name)
        ## This advertises to the controller that we're done
        con$RPUSH(key_complete, id)
        self$log(paste0("TASK_", task_status), id)
      })
    },

    print_info=function() {
      banner(sprintf("_- %s! -_", .packageName))
      dat <- list(redis_host=self$con$context$host,
                  redis_port=self$con$context$port,
                  worker=self$name,
                  queue_name=self$queue_name,
                  hostname=Sys.info()[["nodename"]],
                  pid=Sys.getpid(),
                  message=self$keys$message,
                  log=self$keys$log)

      n <- nchar(names(dat))
      pad <- vcapply(max(n) - n, strrep, str=" ")
      ret <- sprintf("\t%s:%s %s",
                     self$styles$key(names(dat)), pad,
                     self$styles$value(as.character(dat)))
      message(paste(ret, collapse="\n"))
    },

    ## TODO: organise doing this on finalisation:
    shutdown=function(status="OK") {
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

## The message passing is really simple minded; it doesn't do
## bidirectional messaging at all yet because that's hard to get right
## from the controller.
##
## Eventually that would be something that would be useful, but it'll
## get another name I think.
run_message_STOP <- function(worker, args) {
  stop(WorkerStop(worker, args))
}

run_message_EVAL <- function(args) {
  print(try(eval(parse(text=args), .GlobalEnv)))
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

worker_styles <- function() {
  list(info=crayon::make_style("grey"),
       key=crayon::make_style("gold"),
       value=crayon::make_style("dodgerblue2"))
}
