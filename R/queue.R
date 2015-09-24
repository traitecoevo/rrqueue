##' Create an rrqueue queue.  A queue requires a queue name and a set
##' of packages and sources to load.  The sources and packages
##' together define an "environment"; on the worker these packages
##' will be loaded and source files will be \code{source}-ed.
##'
##' The default values for \code{redis_host} and \code{redis_port}
##' correspond to Redis' defaults; if your Redis server is configured
##' differently or available over an internet connection you will need
##' to adjust these accordingly.
##'
##' The \code{queue} objects can be created and desroyed at will; all
##' the data is stored on the server.  Once a queue is created it can
##' also be connected to by the \code{observer} object for read-only
##' access.
##'
##' @template queue_methods
##' @title Create an rrqueue queue
##' @param queue_name Queue name (scalar character)
##' @param packages Optional character vector of packages to load
##' @param sources Optional character vector of files to source
##' @param redis_host Redis hostname
##' @param redis_port Redis port number
##' @param global Source files into the global environment?  This is a
##'   good idea for working with the "user friendly" functions.  See
##'   issue 2 on github.
##' @param config Configuration file of key/value pairs in yaml
##'   format.  See the package README for an example.  If given,
##'   additional arguments to this function override values in the
##'   file which in turn override defaults of this function.
##' @export
queue <- function(queue_name, packages=NULL, sources=NULL,
                  redis_host="127.0.0.1", redis_port=6379, global=TRUE,
                  config=NULL) {
  if (!is.null(config)) {
    given <- as.list(sys.call())[-1] # -1 is the function name
    dat <- modifyList(load_config(config), given)
    if (is.null(dat$queue_name)) {
      stop("queue_name must be given or specified in config")
    }
    queue(dat$queue_name, dat$packages, dat$sources,
          dat$redis_host, dat$redis_port, global, NULL)
  } else {
    .R6_queue$new(queue_name, packages, sources, redis_host, redis_port, global)
  }
}

.R6_queue <- R6::R6Class(
  "queue",

  inherit=.R6_observer,

  public=list(
    envir=NULL,
    envir_id=NULL,
    scripts=NULL,

    initialize=function(queue_name, packages, sources, redis_host,
                        redis_port, global) {
      super$initialize(queue_name, redis_host, redis_port)
      self$scripts <- rrqueue_scripts(self$con)
      existing <- self$con$SISMEMBER(self$keys$rrqueue_queues, self$queue_name)
      if (existing == 1) {
        message("reattaching to existing queue")
      } else {
        message("creating new queue")
        ## This cleans up any leftover bits from a previous queue.
        ## It's potentially dangerous.
        queue_clean(self$con, self$keys$queue_name)
      }
      ## NOTE: this is not very accurate because it includes stale
      ## worker keys.
      ##
      ## TODO: A better way of doing this might be to get the message
      ## queue and have the workers respond.  *However* that really
      ## needs to be done on the heartbeat queue perhaps because it
      ## would miss the ones that are currently working on jobs.
      ##
      ## Alternatively, and more simply, we could check for hearbeat
      ## keys (that would be much simpler if a heartbeat was
      ## compulsary).
      message(sprintf("%d workers available", self$workers_len()))

      self$con$SADD(self$keys$rrqueue_queues, self$queue_name)
      self$initialize_environment(packages, sources, global)
      self$keys$tasks <- rrqueue_key_queue(self$queue_name, self$envir_id)
    },

    initialize_environment=function(packages, sources, global=TRUE) {
      if (!is.null(self$envir)) {
        stop("objects environments are immutable(-ish)")
      }
      ## First, we need to load this environment ourselves.
      envir <- new.env(parent=baseenv())
      source_files <- create_environment2(packages, sources, envir, global)

      dat <- list(packages=packages,
                  sources=sources,
                  source_files=source_files)

      dat_str <- object_to_string(dat)
      self$envir <- envir
      self$envir_id <- hash_string(dat_str)

      is_new <- self$con$HSET(self$keys$envirs_contents, self$envir_id, dat_str)
      if (is_new == 1) {
        ## Only store files and send message if the environment is new
        ## to the queue:
        file_info <- object_to_string(files_pack(self$files,
                                                 files=names(source_files)))
        self$con$HSET(self$keys$envirs_files, self$envir_id, file_info)
        self$send_message("ENVIR", self$envir_id)
      }
    },

    ## TODO: envir should be parent.frame?
    enqueue=function(expr, envir=parent.frame(), key_complete=NULL, group=NULL) {
      self$enqueue_(substitute(expr), envir, key_complete, group=group)
    },

    enqueue_=function(expr, envir=parent.frame(), key_complete=NULL, group=NULL) {
      dat <- prepare_expression(expr)

      tmp <- self$scripts("job_incr", self$keys$tasks_counter, character(0))
      task_id <- as.character(tmp[[1]])
      time <- format_redis_time(tmp[[2]])

      expr_str <- save_expression(dat, task_id, envir, self$objects)
      if (is.null(key_complete)) {
        key_complete <- rrqueue_key_task_complete(self$queue_name, task_id)
      }
      keys <- c(self$keys$tasks_expr,
                self$keys$tasks_envir,
                self$keys$tasks_complete,
                self$keys$tasks_status,
                self$keys$tasks_time_sub,
                if (!is.null(group)) self$keys$tasks_group)
      vals <- c(expr_str, self$envir_id, key_complete, TASK_PENDING, time,
                group)
      self$scripts("job_submit",
                   c(self$keys$tasks, keys),
                   c(task_id,        vals))
      invisible(task(self, task_id, key_complete))
    },

    requeue=function(task_id) {
      con <- self$con
      keys <- self$keys

      status <- con$HGET(keys$tasks_status, task_id)
      if (status != TASK_ORPHAN) {
        stop("Can only reqeueue orphaned tasks")
      }

      ## TODO: The migration should happen in a lua script.
      expr_str     <- con$HGET(keys$tasks_expr,     task_id)
      envir_id     <- con$HGET(keys$tasks_envir,    task_id)
      key_complete <- con$HGET(keys$tasks_complete, task_id)

      task2_id <- as.character(con$INCR(keys$tasks_counter))

      key_complete_orphan <- paste0(key_complete, ":orphan")
      time <- redis_time(con)
      redis_multi(con, {
        ## information about the old, abandoned job:
        con$HSET(keys$tasks_complete, task_id, key_complete_orphan)
        con$HSET(keys$tasks_redirect, task_id, task2_id)
        con$HSET(keys$tasks_status,   task_id, TASK_REDIRECT)
        ## information for the new job
        con$HSET(keys$tasks_expr,     task2_id, expr_str)
        con$HSET(keys$tasks_envir,    task2_id, envir_id)
        con$HSET(keys$tasks_complete, task2_id, key_complete)
        con$HSET(keys$tasks_status,   task2_id, TASK_PENDING)
        con$HSET(keys$tasks_time_sub, task2_id, time)
        con$RPUSH(keys$tasks,         task2_id)
      })
      task(self, task2_id, key_complete)
    },

    ## These messages are *broadcast* commands.  No data will be
    ## returned by the worker.  If the worker is omitted, all workers
    ## get the message.
    send_message=function(command, args=NULL, worker_ids=NULL) {
      queue_send_message(self$con, self$keys, command, args, worker_ids)
    },

    has_responses=function(message_id, worker_ids=NULL) {
      if (is.null(worker_ids)) {
        worker_ids <- self$workers_list()
      }
      res <- vnapply(rrqueue_key_worker_response(self$queue_name, worker_ids),
                     self$con$HEXISTS, message_id)
      setNames(as.logical(res), worker_ids)
    },

    get_responses=function(message_id, worker_ids=NULL, delete=FALSE, wait=0) {
      get_responses(self$con, self$keys, message_id, worker_ids, delete, wait)
    },

    get_response=function(message_id, worker_id, delete=FALSE, wait=0) {
      self$get_responses(message_id, worker_id, delete, wait)[[1]]
    },

    response_ids=function(worker_id) {
      response_keys <- rrqueue_key_worker_response(self$queue_name, worker_id)
      ids <- as.character(self$con$HKEYS(response_keys))
      ids[order(as.numeric(ids))]
    },

    ## This one is tricky.  Responses will go into one list per
    ## worker, and matching up with the id requires fetching the whole
    ## thing.  So let's change the response queue to be a hash on ID.
    ## fetch_response=function
    tasks_drop=function(task_ids) {
      con <- self$con
      keys <- self$keys

      status <- self$tasks_status(task_ids)
      if (any(status == TASK_RUNNING)) {
        stop("One of the tasks is running -- not clear how to deal")
      }

      ret <- logical(length(task_ids))
      names(ret) <- task_ids

      ## TODO: Total race condition here because the queue status
      ## might have switched between these calls.  This needs to be
      ## done in Lua to get it right I think, or we'll risk losing
      ## tasks (and potentially quite a few because of the way that
      ## this works).
      ##
      ## Alternatively, we can run WATCH first, but that does not seem
      ## to work reliably.
      ##
      ## TODO: also remove redirects or deal with the case when this
      ## needs redirect.  Basically this is not very robust.
      redis_multi(con, {
        for (i in task_ids[status == TASK_PENDING]) {
          ret[[i]] <- self$con$LREM(self$keys$tasks, 0, i) > 0L
        }
        con$HDEL(keys$tasks_expr,     task_ids)
        con$HDEL(keys$tasks_status,   task_ids)
        con$HDEL(keys$tasks_envir,    task_ids)
        con$HDEL(keys$tasks_complete, task_ids)
        con$HDEL(keys$tasks_group,    task_ids)
        con$HDEL(keys$tasks_result,   task_ids)
        con$HDEL(keys$tasks_time_sub, task_ids)
        con$HDEL(keys$tasks_time_beg, task_ids)
        con$HDEL(keys$tasks_time_end, task_ids)
        con$HDEL(keys$tasks_worker,   task_ids)
      })

      ret
    },

    files_pack=function(..., files=c(...)) {
      files_pack(self$files, files=files)
    },
    files_unpack=function(pack, path=tempfile()) {
      files_unpack(self$files, pack, path)
    },

    tasks_set_group=function(task_ids, group, exists_action="stop") {
      tasks_set_group(self$con, self$keys, task_ids, group, exists_action)
    },

    stop_workers=function(worker_ids=NULL, kill_local=FALSE, wait_stop=1) {
      stop_workers(self$con, self$keys, worker_ids, kill_local, wait_stop)
    }
  ))

queue_clean <- function(con, queue_name, purge=FALSE,
                        stop_workers=FALSE,
                        kill_local=FALSE,
                        wait_stop=1) {
  keys <- rrqueue_keys(queue_name)
  if (stop_workers) {
    stop_workers(con, keys, NULL, kill_local, wait_stop)
  }
  con$SREM(keys$rrqueue_queues, keys$queue_name)

  if (purge) {
    scan_del(con, paste0(queue_name, "*"))
  } else {
    ## TODO: This one here seems daft.  If there are workers they
    ## might still be around, and they might be working on tasks.
    ## Might be best not to get too involved with modifying the
    ## worker queue, aside from messaging, really; leave deleting
    ## worker queues to a standalone function?
    ##
    ##   con$DEL(keys$workers_name)
    ##   con$DEL(keys$workers_status)
    ##   con$DEL(keys$workers_task)

    con$DEL(keys$tasks)
    con$DEL(keys$tasks_counter)
    con$DEL(keys$tasks_expr)
    con$DEL(keys$tasks_status)
    con$DEL(keys$tasks_result)
    con$DEL(keys$tasks_envir)
    con$DEL(keys$tasks_time_sub)
    con$DEL(keys$tasks_time_beg)
    con$DEL(keys$tasks_time_end)

    con$DEL(keys$envirs_contents)
  }
}

queue_send_message <- function(con, keys, command, args=NULL,
                               worker_ids=NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  ## TODO: check if the worker exists before pushing anything onto
  ## its message queue.
  ##
  ## In theory, this could be done with RPUSHX and checking for a
  ## 0 return.  But that's not quite right because I'd want to
  ## check for the worker elsewhere (the message queue will be
  ## empty at *some* point).  Worse, this could leave messages
  ## pushed for only some of the workers.
  ##
  ## On the other hand, pushing unneeded messages is not a big
  ## problem, so I'm inclined to leave it for now.
  key <- rrqueue_key_worker_message(keys$queue_name, worker_ids)
  message_id <- redis_time(con)
  content <- message_prepare(message_id, command, args)
  for (k in key) {
    con$RPUSH(k, content)
  }
  invisible(message_id)
}

get_responses <- function(con, keys, message_id, worker_ids=NULL,
                          delete=FALSE, wait=0, every=0.05) {
  ## NOTE: this won't work well if the message was sent only to a
  ## single worker, or a worker who was not yet started.
  ##
  ## NOTE: Could do a progress bar easily enough here.
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  response_keys <- rrqueue_key_worker_response(keys$queue_name, worker_ids)
  res <- poll_hash_keys(con, response_keys, message_id, wait, every)

  msg <- vlapply(res, is.null)
  if (any(msg)) {
    stop(paste0("Response missing for workers: ",
                paste(worker_ids[msg], collapse=", ")))
  }
  if (delete) {
    for (k in response_keys) {
      con$HDEL(k, message_id)
    }
  }

  names(res) <- worker_ids
  lapply(res, function(x) string_to_object(x)$result)
}

stop_workers <- function(con, keys, worker_ids=NULL,
                         kill_local=FALSE, wait_stop=1) {
  worker_ids <- workers_list(con, keys)
  if (length(worker_ids) == 0L) {
    return(invisible())
  }
  message_id <- queue_send_message(con, keys, "STOP", worker_ids=worker_ids)

  ## TODO: would be nice if this could retrieve actual messages, but
  ## that requires getting missing message support into get_responses.
  if (kill_local) {
    ## Get the set of workers:
    if (wait_stop > 0 && length(workers_list(con, keys)) > 0) {
      ## Give the workers a second to cleanup
      message("Waiting for local workers to stop themselves")
      Sys.sleep(wait_stop)
    }
    extant <- workers_list(con, keys)
    stopped <- setdiff(worker_ids, extant)
    if (length(stopped) > 0L) {
      message(sprintf("%d workers stopped cleanly: %s",
                      length(stopped), paste(stopped, collapse=", ")))
    }
    worker_ids <- intersect(worker_ids, extant)

    if (length(worker_ids) > 0) {
      w_info <- workers_info(con, keys, worker_ids)
      w_local <- worker_ids[vcapply(w_info, "[[", "hostname") == hostname()]
      n_local <- length(w_local)
      if (n_local > 0) {
        w_local_pid <- vnapply(w_info[w_local], "[[", "pid")
        message(sprintf("killing %d workers: %s",
                        length(w_local), paste(w_local, collapse=", ")))
        tools::pskill(w_local_pid, tools::SIGKILL)
        ## Some attempt at cleanup:
        for (worker_id in w_local) {
          worker_cleanup(con, keys, worker_id)
        }
      }
    }
  }
}
