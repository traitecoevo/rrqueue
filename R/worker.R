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
    key_queue=NULL,
    envir=list(),
    heartbeat=NULL,
    heartbeat_period=NULL,
    heartbeat_expire=NULL,
    name=NULL,
    files=NULL,
    objects=NULL,
    styles=NULL,

    initialize=function(queue_name, redis_host, redis_port,
      heartbeat_period, heartbeat_expire, key_worker_alive) {
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

      withCallingHandlers(self$initialize_worker(key_worker_alive),
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
    initialize_worker=function(key_worker_alive) {
      info <- object_to_string(self$print_info())

      self$heartbeat <- heartbeat(self$con, self$keys$heartbeat,
                                  self$heartbeat_period,
                                  self$heartbeat_expire)

      redis_multi(self$con, {
        self$con$SADD(self$keys$workers_name,   self$name)
        self$con$HSET(self$keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(self$keys$workers_task,   self$name)
        self$con$DEL(self$keys$log)
        self$con$HSET(self$keys$workers_info,   self$name, info)
      })
      self$log("ALIVE")

      self$files <- file_cache(self$keys$files, self$con)
      self$objects <- object_cache(self$keys$objects, self$con)

      ## Always listen to the message queue, even if no environments
      ## will be loaded (this could be merged with the loop below for
      ## better robustness).
      self$key_queue <- self$keys$message

      ## load *existing* environments that the controller knows about.
      envir_ids <- as.character(self$con$HKEYS(self$keys$envirs_contents))
      for (envir_id in envir_ids) {
        self$initialize_environment(envir_id)
      }

      ## This announces that we're up; things may monitor this
      ## queue, and worker_spawn does a BLPOP to
      if (!is.null(key_worker_alive)) {
        self$con$RPUSH(key_worker_alive, self$name)
      }
    },

    initialize_environment=function(envir_id) {
      ## TODO: for now, this assumes that all files are found in the
      ## appropriate directory and will just go for it.  Future
      ## versions will be more clever here and load files from Redis
      ## into a temporary directory and source from there.  At the
      ## same time deal with the error here; it's no longer a deal
      ## breaker.
      self$log("ENVIR", envir_id)
      dat_str <- self$con$HGET(self$keys$envirs_contents, envir_id)
      dat <- string_to_object(dat_str)

      ## TODO: Probably refactor this into something easily testable...
      ## TODO: avoid the failure here
      ## Check the hashes of the files
      hash_expected <- dat$source_files
      if (compare_hash(hash_expected)) {
        e <- create_environment(dat$packages, dat$sources)
      } else {
        tmp <- tempfile("rrqueue_")
        files_unpack(self$files, hash_expected, tmp)
        owd <- setwd(tmp)
        e <- tryCatch(create_environment(dat$packages, dat$sources),
                      finally=setwd(owd))
      }

      self$envir[[envir_id]] <- e
      ## Here; can do a bit better:
      fmt <- function(x) {
        if (is.null(x)) {
          "(none)"
        } else {
          paste(x, collapse=" ")
        }
      }

      ## Read from the message queue *first* as that allows a STOP
      ## command to prevent the worker continuing with job.
      self$key_queue <- c(self$keys$message,
                          rrqueue_key_queue(self$queue_name, names(self$envir)))
      self$con$SADD(self$keys$envir, envir_id)

      self$log("ENVIR PACKAGES", fmt(dat$packages), push=FALSE)
      self$log("ENVIR SOURCES",  fmt(dat$sources),  push=FALSE)
      TRUE
    },

    get_environment=function(envir_id) {
      self$envir[[envir_id]]
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
        msg_log <- sprintf("%d %s %s", ti, label, paste(message, collapse="\n"))
        ## Try and make nicely printing logs for the case where the
        ## message length is longer than 1:
        lab <- c(label, rep_len(blank(nchar(label)), length(message) - 1L))
        msg_scr <- paste(sprintf("[%s] %s %s", ts,
                                 self$styles$key(lab),
                                 self$styles$value(message)),
                         collapse="\n")
      }
      message(msg_scr)
      if (push) {
        self$con$RPUSH(self$keys$log, msg_log)
      }
    },

    ## TODO: Store time since last task.
    main=function() {
      con <- self$con

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
        task <- con$BLPOP(self$key_queue, self$heartbeat_period)
        if (is.null(task)) {
          ## self$log("WAITING", push=FALSE)
        } else {
          channel <- task[[1]]
          if (channel == self$keys$message) {
            self$run_message(task[[2]])
          } else { # is a task
            withCallingHandlers(
              self$run_task(task[[2]]),
              WorkerError=function(e)
                self$task_cleanup(e, e$task_id, e$task_status))
          }
        }
      }
    },

    run_task=function(task_id) {
      keys <- self$keys
      con <- self$con
      self$log("TASK_START", task_id)

      expr <- self$task_retrieve(task_id)
      context <- self$task_prepare(task_id, expr)

      ## Here, we get time from the Redis server, not R; that means
      ## that all ideas of time are centralised.
      time <- redis_time(con)
      redis_multi(con, {
        con$HSET(keys$workers_status, self$name, WORKER_BUSY)
        con$HSET(keys$workers_task,   self$name, task_id)
        con$HSET(keys$tasks_worker,   task_id,   self$name)
        con$HSET(keys$tasks_status,   task_id,   TASK_RUNNING)
        con$HSET(keys$tasks_time_beg, task_id,   time)
      })

      expr_str <- capture.output(print(context$expr))
      self$log("EXPR", expr_str, push=FALSE)

      res <- tryCatch(eval(context$expr, context$envir),
                      error=WorkerTaskError)

      if (inherits(res, "WorkerTaskError")) {
        status <- TASK_ERROR
      } else {
        status <- TASK_COMPLETE
      }
      self$task_cleanup(res, task_id, status)
    },

    run_message=function(msg) {
      content <- string_to_object(msg)
      message_id <- content$id
      cmd <- content$command
      args <- content$args

      ## NOTE: This is a departure from previous because we no longer
      ## print the *arguments* to args.  That could be modified into
      ## here pretty easily by appending args iff they are a scalar
      ## character.  Better might be to serialise to json here, but
      ## that's going to be more work and not work for everything, not
      ## necessarily transitive without assumptions and YAGNI.
      self$log("MESSAGE", cmd)

      ## TODO: purge object cache (save on memory)
      ## TODO: file(s) get, put (debugging, deployment)
      ## TODO: environment load, purge, etc.
      ## TODO: worker restart?  Possible?
      res <- switch(cmd,
                    PING=run_message_PING(),
                    ECHO=run_message_ECHO(args),
                    EVAL=run_message_EVAL(args),
                    STOP=run_message_STOP(self, message_id, args), # noreturn
                    INFO=run_message_INFO(self),
                    ENVIR=run_message_ENVIR(self, args),
                    PUSH=run_message_PUSH(self, args),
                    PULL=run_message_PULL(self, args),
                    DIR=run_message_DIR(args),
                    run_message_unknown(cmd, args))

      self$send_response(message_id, cmd, res)
    },

    send_response=function(message_id, cmd, result) {
      self$con$HSET(self$keys$response, message_id,
                    response_prepare(message_id, cmd, result))
    },

    task_retrieve=function(task_id) {
      expr_stored <- self$con$HGET(self$keys$tasks_expr, task_id)
      if (is.null(expr_stored)) {
        stop(WorkerTaskMissing(self, task_id))
      }
      expr_stored
    },

    task_prepare=function(task_id, expr_stored) {
      envir_id <- self$con$HGET(self$keys$tasks_envir, task_id)
      envir <- new.env(parent=self$get_environment(envir_id))
      expr <- restore_expression(expr_stored, envir, self$objects)
      list(expr=expr, envir=envir)
    },

    task_cleanup=function(data, task_id, task_status) {
      con <- self$con
      keys <- self$keys
      key_complete <- con$HGET(keys$tasks_complete, task_id)
      time <- redis_time(con)
      redis_multi(con, {
        con$HSET(keys$tasks_result,   task_id,   object_to_string(data))
        con$HSET(keys$tasks_status,   task_id,   task_status)
        con$HSET(keys$tasks_time_end, task_id,   time)
        con$HSET(keys$workers_status, self$name, WORKER_IDLE)
        con$HDEL(keys$workers_task,   self$name)
        ## This advertises to the controller that we're done
        con$RPUSH(key_complete, task_id)
        self$log(paste0("TASK_", task_status), task_id)
      })
    },

    print_info=function() {
      print(worker_info(self), banner=TRUE, styles=self$styles)
    },

    shutdown=function(status="OK") {
      self$heartbeat$stop()
      worker_cleanup(self$con, self$keys, self$name)
      self$log("STOP", status)
    }))

##' Create an rrqueue worker.  This blocks the main loop.
##' @title Create an rrqueue worker
##' @param queue_name Queue name
##' @param redis_host Host name/IP for the Redis server
##' @param redis_port Port for the Redis server
##' @param heartbeat_period Period between heartbeat pulses
##' @param heartbeat_expire Time that heartbeat pulses will persist
##'   for (must be greater than \code{heartbeat_period})
##' @param key_worker_alive Optional key to write to when the worker
##'   becomes alive.  The worker will push onto this key so that
##'   another process can monitor it and determine when a worker has
##'   come up.
##' @export
worker <- function(queue_name,
                   redis_host="127.0.0.1", redis_port=6379,
                   heartbeat_period=30,
                   heartbeat_expire=heartbeat_period * 3,
                   key_worker_alive=NULL) {
  .R6_worker$new(queue_name, redis_host, redis_port,
                 heartbeat_period, heartbeat_expire, key_worker_alive)
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

workers_list_exited <- function(con, keys) {
  active <- workers_list(con, keys)
  fmt <- "%s:workers:%s:log"
  all <- RedisAPI::scan_find(con, sprintf(fmt, keys$queue_name, "*"))
  ids <- sub(sprintf(fmt, keys$queue_name, "(.*)"), "\\1", all)
  setdiff(ids, active)
}

workers_status <- function(con, keys, worker_ids=NULL) {
  from_redis_hash(con, keys$workers_status, worker_ids)
}

workers_times <- function(con, keys, worker_ids=NULL, unit_elapsed="secs") {
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
  if (nrow(log) > 0L) {
    t_last <- log$time[match(worker_ids, log$worker_id)]
  } else {
    t_last <- rep_len(NA_real_, length(worker_ids))
  }
  t_curr <- as.numeric(redis_time(con))

  data.frame(worker_id=worker_ids,
             expire_max=expire_max,
             expire=t_expire,
             last_seen=as.numeric(expire_max - t_expire, unit_elapsed),
             last_action=as.numeric(t_curr - t_last, unit_elapsed),
             stringsAsFactors=FALSE)
}

worker_log_tail <- function(con, keys, worker_id, n=1) {
  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  log_key <- rrqueue_key_worker_log(keys$queue_name, worker_id)
  parse_worker_log(as.character(con$LRANGE(log_key, -n, -1)))
}

workers_log_tail <- function(con, keys, worker_ids=NULL, n=1) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  tmp <- lapply(worker_ids, function(i) worker_log_tail(con, keys, i, n))
  if (length(tmp) > 0L) {
    n <- viapply(tmp, nrow)
    cbind(worker_id=rep(worker_ids, n), do.call("rbind", tmp, quote=TRUE))
  } else {
    ## NOTE: Need to keep this in sync with parse_worker_log; get some
    ## tests in here to make sure...
    data.frame(worker_id=worker_ids, time=character(0),
               command=character(0), message=character(0),
               stringsAsFactors=FALSE)
  }
}

workers_task_id <- function(con, keys, worker_id) {
  from_redis_hash(con, keys$workers_task, worker_id)
}

worker_overview <- function(con, keys) {
  lvls <- c(WORKER_IDLE, WORKER_BUSY, WORKER_LOST)
  status <- workers_status(con, keys)
  table(factor(status, lvls))
}

worker_envir <- function(con, keys, worker_id) {
  key <- rrqueue_key_worker_envir(keys$queue_name, worker_id)
  as.character(con$SMEMBERS(key))
}

run_message_PING <- function() {
  message("PONG")
  "PONG"
}

run_message_ECHO <- function(msg) {
  message(msg)
  "OK"
}

run_message_EVAL <- function(args) {
  print(try(eval(parse(text=args), .GlobalEnv)))
}

run_message_STOP <- function(worker, message_id, args) {
  worker$send_response(message_id, "STOP", "BYE")
  stop(WorkerStop(worker, args))
}

run_message_INFO <- function(worker) {
  info <- worker$print_info()
  worker$con$HSET(worker$keys$workers_info, worker$name,
                  object_to_string(info))
  info
}

run_message_ENVIR <- function(worker, args) {
  if (worker$initialize_environment(args)) {
    "ENVIR OK"
  } else {
    "ENVIR ERROR"
  }
}

## Push and pull
run_message_PUSH <- function(worker, args) {
  ## Push files from the worker into the DB.
  files_pack(worker$files, files=args)
}

run_message_PULL <- function(worker, args) {
  envir_id <- args
  ok <- length(envir_id) == 1 &&
               worker$con$HEXISTS(worker$keys$envirs_contents, envir_id)
  if (ok) {
    dat_str <- worker$con$HGET(worker$keys$envirs_contents, envir_id)
    dat <- string_to_object(dat_str)
    hash_expected <- dat$source_files
    if (!compare_hash(hash_expected)) {
      files_unpack(worker$files, hash_expected)
    }
    worker$log("PULL", "OK")
    "OK"
  } else {
    worker$log("PULL", "FAIL")
    "FAIL"
  }
}

run_message_DIR <- function(args) {
  if (length(args) == 0L) {
    args <- list()
  }
  res <- try(do.call("dir", args))
  if (!inherits(res, "try-error")) {
    path <- if (is.null(args$path)) res else file.path(args$path, res)
    ret <- setNames(rep(NA_character_, length(res)), res)
    is_file <- !vlapply(path, is_directory, USE.NAMES=FALSE)
    ret[is_file] <- hash_files(path[is_file])
  }
  ret
}

run_message_unknown <- function(cmd, args) {
  msg <- sprintf("Recieved unknown message: [%s] [%s]", cmd, args)
  message(msg)
  structure(list(message=msg, cmd=cmd, args=args),
            class=c("condition"))
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

worker_info <- function(worker) {
  dat <- list(version=version_string(),
              hostname=Sys.info()[["nodename"]],
              pid=Sys.getpid(),
              redis_host=worker$con$host,
              redis_port=worker$con$port,
              worker=worker$name,
              queue_name=worker$queue_name,
              heartbeat_period=worker$heartbeat_period,
              heartbeat_expire=worker$heartbeat_expire,
              message=worker$keys$message,
              response=worker$keys$response,
              envir=as.character(worker$con$SMEMBERS(worker$keys$envir)),
              log=worker$keys$log)
  class(dat) <- "worker_info"
  dat
}

##' @export
print.worker_info <- function(x, banner=FALSE, styles=worker_styles(), ...) {
  xx <- x
  xx$envir <- sprintf("{%s}", paste(x$envir, collapse=", "))
  n <- nchar(names(xx))
  pad <- vcapply(max(n) - n, strrep, str=" ")
  ret <- sprintf("    %s:%s %s",
                 styles$key(names(xx)), pad,
                 styles$value(as.character(xx)))
  if (banner) {
    message(crayon::make_style(random_colour())(worker_banner_text()))
  }
  message(paste(ret, collapse="\n"))
  invisible(x)
}

worker_cleanup <- function(con, keys, worker_id) {
  con$DEL(rrqueue_key_worker_heartbeat(keys$queue_name, worker_id))
  con$SREM(keys$workers_name,   worker_id)
  con$HDEL(keys$workers_status, worker_id)
}

workers_info <- function(con, keys, worker_ids=NULL) {
  from_redis_hash(con, keys$workers_info, worker_ids,
                  f=Vectorize(string_to_object, SIMPLIFY=FALSE))
}

## TODO: this is not really the complement of worker_envir; This
## returns a true/false vector over workers, while worker_envir
## returns a character vector of environments that a worker can do.
## Not sure that's 100% desirable.
envir_workers <- function(con, keys, envir_id, worker_ids=NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  keys <- rrqueue_key_worker_envir(keys$queue_name, worker_ids)
  ret <- vnapply(keys, con$SISMEMBER, envir_id)
  storage.mode(ret) <- "logical"
  names(ret) <- worker_ids
  ret
}
