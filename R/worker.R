WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_LOST <- "LOST"

## TODO: Part way through restructuring the task timeout / heartbeat
## thing, but it might be wise to have the timeout be slower during
## the idle phase?  That could be done with a multiplier on the
## heartbeat.

## TODO: Printing WAITING every 30 seconds is going to get annoying
## really quickly; might be worth working out how convey this
## information more politely.

##' @importFrom R6 R6Class
.R6_worker <- R6::R6Class(
  "worker",

  public=list(
    con=NULL,
    queue_name=NULL,
    keys=NULL,
    envir=list(),
    heartbeat=NULL,
    heartbeat_period=NULL,
    heartbeat_expire=NULL,
    name=NULL,
    objects=NULL,
    styles=NULL,

    initialize=function(queue_name, redis_host, redis_port,
      heartbeat_period, heartbeat_expire) {
      #
      self$name <- sprintf("%s::%d", hostname(), process_id())
      self$queue_name <- queue_name
      self$heartbeat_period <- as.numeric(heartbeat_period)
      self$heartbeat_expire <- as.numeric(heartbeat_expire)

      self$keys <- rrqueue_keys(self$queue_name, worker_name=self$name)
      self$styles <- worker_styles()

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

      ## Establish the database connection
      self$con <- redis_connection(redis_host, redis_port)

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

      withCallingHandlers(self$initialize_worker(),
                          error=catch_error)
      ## The problem is that here, withCallingHandlers will let us
      ## continue after clearing the WorkerStop error onto the error
      ## error, so we get a OK/STOP pair.
      tryCatch(self$main(),
               WorkerStop=catch_worker_stop,
               error=catch_error,
               interrupt=catch_interrupt)
    },

    ## This is in its own function so that error handling can be done
    ## gracefully; it's only called by initialize()
    initialize_worker=function() {
      self$print_info()

      self$heartbeat <- heartbeat(self$con, self$keys$heartbeat,
                                  self$heartbeat_period,
                                  self$heartbeat_expire)

      redis_multi(self$con, {
        self$con$SADD(self$keys$workers_name,   self$name)
        self$con$HSET(self$keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(self$keys$workers_task,   self$name)
        self$con$DEL(self$keys$log)
        self$log("ALIVE")
        ## This announces that we're up; things may monitor this
        ## queue, and worker_spawn does a BLPOP to
        self$con$RPUSH(self$keys$workers_new,   self$name)
      })
      self$objects <- object_cache(self$con, self$keys$objects)
    },

    ## This gets called at the beginning of a job.
    initialize_environment=function(id) {
      e <- self$envir[[id]]
      if (is.null(e)) {
        self$log("ENVIR", id)
        keys <- self$keys
        dat_str <- self$con$HGET(self$keys$envirs_contents, id)
        dat <- string_to_object(dat_str)

        ## Check the hashes of the files
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
      con <- self$con
      key_queue_tasks <- self$keys$tasks_id
      key_queue_msg  <- self$keys$message
      ## Read from the message queue *first* as that allows a STOP
      ## command to prevent the worker continuing with job.
      key_queue <- c(key_queue_msg, key_queue_tasks)

      ## TODO: should be possible to send SIGHUP or something to
      ## trigger stopping current task but keep listening.
      ##
      ## TODO: another option is to look at a redis key after
      ## interrupt?
      ##
      ## TODO: hopefully can do some of that with the heartbeat.
      ## Remaining question is how long to poll between jobs here?  No
      ## real need for it to be the same as the heartbeat period but
      ## might as well make it so.
      repeat {
        task <- con$BLPOP(key_queue, self$heartbeat_period)
        if (is.null(task)) {
          self$log("WAITING", push=FALSE)
        } else {
          channel <- task[[1]]
          if (channel == key_queue_tasks) {
            withCallingHandlers(
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
      self$log("TASK_START", id)

      expr <- self$task_retrieve(id)

      context <- withCallingHandlers(
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
        con$HSET(keys$tasks_time_beg, id,        time)
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
        con$HSET(keys$tasks_time_end, id,        time)
        con$HSET(keys$workers_status, self$name, WORKER_IDLE)
        con$HDEL(keys$workers_task,   self$name)
        ## This advertises to the controller that we're done
        con$RPUSH(key_complete, id)
        self$log(paste0("TASK_", task_status), id)
      })
    },

    print_info=function() {
      message(crayon::make_style(random_colour())(worker_banner_text()))
      dat <- list(version=version_string(),
                  hostname=Sys.info()[["nodename"]],
                  pid=Sys.getpid(),
                  redis_host=self$con$host,
                  redis_port=self$con$port,
                  worker=self$name,
                  queue_name=self$queue_name,
                  heartbeat_period=self$heartbeat_period,
                  heartbeat_expire=self$heartbeat_expire,
                  message=self$keys$message,
                  log=self$keys$log)

      n <- nchar(names(dat))
      pad <- vcapply(max(n) - n, strrep, str=" ")
      ret <- sprintf("    %s:%s %s",
                     self$styles$key(names(dat)), pad,
                     self$styles$value(as.character(dat)))
      message(paste(ret, collapse="\n"))
    },

    ## TODO: organise doing this on finalisation:
    shutdown=function(status="OK") {
      self$heartbeat$stop()
      self$con$DEL(self$keys$heartbeat)
      self$con$SREM(self$keys$workers_name,   self$name)
      self$con$HDEL(self$keys$workers_status, self$name)
      self$log("STOP", status)
    }))

##' Create an rrqueue worker.  This blocks the main loop.
##' @title Create an rrqueue worker
##' @param queue_name Queue name
##' @param redis_host Host name/IP for the Redis server
##' @param redis_port Port for the Redis server
##' @param heartbeat_period Period between heartbeat pulses
##' @param heartbeat_expire Time that heartbeat pulses will persist
##' for (must be greater than \code{heartbeat_period})
##' @export
worker <- function(queue_name,
                   redis_host="127.0.0.1", redis_port=6379,
                   heartbeat_period=30,
                   heartbeat_expire=heartbeat_period * 3) {
  .R6_worker$new(queue_name, redis_host, redis_port,
                 heartbeat_period, heartbeat_expire)
}

workers_len <- function(con, keys) {
  ## NOTE: this is going to be an *estimate* because there might
  ## be old workers floating around.
  ##
  ## TODO: Drop orphan workers here, at which point this becomes a
  ## bit slower than we'd like...
  con$SCARD(keys$workers_name)
}

workers_list <- function(con, keys) {
  as.character(con$SMEMBERS(keys$workers_name))
}

workers_status <- function(con, keys, worker_ids=NULL) {
  from_redis_hash(con, keys$workers_status, worker_ids)
}

workers_status_time <- function(con, keys, worker_ids=NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  f_expire_max <- function(key) {
    t <- con$GET(key)
    if (is.null(t)) NA_real_ else as.numeric(t)
  }

  key <- rrqueue_key_worker_heartbeat(keys$queue_name, worker_ids)
  expire_max <- vnapply(key, f_expire_max, USE.NAMES=FALSE)

  ## Current time left to expire:
  t_expire <- unname(vnapply(key, con$PTTL))
  t_expire[t_expire > 0] <- t_expire[t_expire > 0] / 1000

  log <- workers_log_tail(con, keys, worker_ids, 1)
  t_last <- log$time[match(worker_ids, log$worker_id)]
  t_curr <- as.numeric(redis_time(con))

  data.frame(worker_id=worker_ids,
             expire_max=expire_max,
             expire=t_expire,
             last_seen=expire_max - t_expire,
             last_action=t_curr - t_last,
             stringsAsFactors=FALSE)
}

worker_log_tail <- function(con, keys, worker_id, n=1) {
  log_key <- rrqueue_key_worker_log(keys$queue_name, worker_id)
  parse_worker_log(as.character(con$LRANGE(log_key, -n, -1)))
}

## TODO: get the id in here!
workers_log_tail <- function(con, keys, worker_ids=NULL, n=1) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  tmp <- lapply(worker_ids, function(i) worker_log_tail(con, keys, i, n))
  n <- viapply(tmp, nrow)
  cbind(worker_id=rep(worker_ids, n), do.call("rbind", tmp, quote=TRUE))
}

workers_task_id <- function(con, keys, worker_id) {
  from_redis_hash(con, keys$workers_task, worker_id)
}

worker_task_get <- function(con, keys, worker_id) {
  task_id <- workers_task_id(con, keys, worker_id)
  task(con, keys$queue_name, task_id)
}

worker_overview <- function(con, keys) {
  lvls <- c(WORKER_IDLE, WORKER_BUSY, WORKER_LOST)
  status <- workers_status(con, keys)
  table(factor(status, lvls))
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

##' @importFrom crayon make_style
worker_styles <- function() {
  list(info=crayon::make_style("grey"),
       key=crayon::make_style("gold"),
       value=crayon::make_style("dodgerblue2"))
}


## To regenerate / change:
##   fig <- rfiglet::figlet(sprintf("_- %s -_", "rrqueue!"), "slant")
##   dput(rstrip(strsplit(as.character(fig), "\n")[[1]]))
worker_banner_text <- function() {
  c("                                                       __",
    "                ______________ ___  _____  __  _____  / /",
    "      ______   / ___/ ___/ __ `/ / / / _ \\/ / / / _ \\/ /  ______",
    "     /_____/  / /  / /  / /_/ / /_/ /  __/ /_/ /  __/_/  /_____/",
    " ______      /_/  /_/   \\__, /\\__,_/\\___/\\__,_/\\___(_)      ______",
    "/_____/                   /_/                              /_____/"
    ) -> txt
  paste(txt, collapse="\n")
}
