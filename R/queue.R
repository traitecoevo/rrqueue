## NOTE: queue objects are be able to be destroyed at will: all the
## data is be stored on the server.
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
        self$clean()
      }
      ## NOTE: this is not very accurate because it includes stale
      ## worker keys.
      ##
      ## TODO: A better way of doing this might be to git the message
      ## queue and have the workers respond.  *However* that really
      ## needs to be done on the heartbeat queue perhaps because it
      ## would miss the ones that are currently working on jobs.
      ##
      ## Alternatively, and more simply, we could check for hearbeat
      ## keys (that would be much simpler if a heartbeat was
      ## compulsary).
      nw <- self$workers_len()
      if (nw > 0L) {
        message(sprintf("%d workers available", nw))
      }

      self$con$SADD(self$keys$rrqueue_queues, self$queue_name)
      self$initialize_environment(packages, sources, TRUE, global)
    },

    clean=function() {
      queue_clean(self$con, self$queue_name)
    },

    ## TODO: facility for named environnents?
    ## TODO: facility for deleting environments?
    ## TODO: What on earth was set_default meant to be used for?
    initialize_environment=function(packages, sources,
                                    set_default=FALSE, global=TRUE) {
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
      self$con$HSET(self$keys$envirs_contents, self$envir_id, dat_str)
    },

    ## TODO: clean up queues on startup, or attach to existing queue?
    ## TODO: spin up workers?
    ## TODO: pending, completed, etc.
    ## TODO: allow setting a "group" or "name" for more easily
    ## recalling jobs?
    ## TODO: envir should be parent.frame?
    enqueue=function(expr, envir=.GlobalEnv, key_complete=NULL, group=NULL) {
      self$enqueue_(substitute(expr), envir, key_complete, group=group)
    },

    enqueue_=function(expr, envir=.GlobalEnv, key_complete=NULL, group=NULL) {
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
                   c(self$keys$tasks_id, keys),
                   c(task_id,            vals))
      invisible(task(self, task_id, key_complete))
    },

    requeue=function(task_id) {
      con <- self$con
      keys <- self$keys

      status <- con$HGET(keys$tasks_status, task_id)
      if (status != TASK_ORPHAN) {
        stop("Can only reqeueue orphaned tasks")
      }

      ## TODO: The migration could happen in a lua script.
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
        con$RPUSH(keys$tasks_id,      task2_id)
      })
      task(self, task2_id, key_complete)
    },

    ## These messages are *broadcast* commands.  No data will be
    ## returned by the worker.  If the worker is omitted, all workers
    ## get the message.
    send_message=function(command, args=NULL, worker_ids=NULL) {
      if (is.null(worker_ids)) {
        worker_ids <- self$workers_list()
      }
      ## TODO: check if the worker exists before pushing anything onto
      ## its message queue.

      ## In theory, this could be done with RPUSHX and checking for a
      ## 0 return.  But that's not quite right because I'd want to
      ## check for the worker elsewhere (the message queue will be
      ## empty at *some* point).  Worse, this could leave messages
      ## pushed for only some of the workers.
      key <- rrqueue_key_worker_message(self$queue_name, worker_ids)
      message_id <- redis_time(self$con)
      content <- message_prepare(message_id, command, args)
      for (k in key) {
        self$con$RPUSH(k, content)
      }
      invisible(message_id)
    },

    has_responses=function(message_id, worker_ids=NULL) {
      if (is.null(worker_ids)) {
        worker_ids <- self$workers_list()
      }
      res <- vnapply(rrqueue_key_worker_response(self$queue_name, worker_ids),
                     self$con$HEXISTS, message_id)
      setNames(as.logical(res), worker_ids)
    },

    get_responses=function(message_id, worker_ids=NULL, delete=FALSE) {
      if (is.null(worker_ids)) {
        worker_ids <- self$workers_list()
      }

      response_keys <- rrqueue_key_worker_response(self$queue_name, worker_ids)
      res <- lapply(response_keys, self$con$HGET, message_id)
      names(res) <- worker_ids

      msg <- vlapply(res, is.null)
      if (any(msg)) {
        message <- paste0("Response missing for workers: ",
                          paste(worker_ids[msg], collapse=", "))
        stop(message)
      }
      if (delete) {
        for (k in response_keys) {
          self$con$HDEL(k, message_id)
        }
      }
      ## NOTE: if missing_action is pass and a result is really
      ## missing, then it will be impossible to detect which elements
      ## are missing based on the output of this.
      res <- lapply(res, function(x) string_to_object(x)$result)
      res
    },

    get_response=function(message_id, worker_id, delete=FALSE) {
      self$get_responses(message_id, worker_id, delete)[[1]]
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

    ## TODO: should the argument be 'task_ids'?
    tasks_drop=function(id) {
      con <- self$con
      keys <- self$keys

      status <- self$tasks_status(id)

      if (any(status == TASK_RUNNING)) {
        stop("One of the tasks is running -- not clear how to deal")
      }

      ret <- logical(length(id))
      names(ret) <- id

      redis_multi(con, {
        for (i in id[status == TASK_PENDING]) {
          ret[[i]] <- self$con$LREM(keys$tasks_id, 0, i) > 0L
        }
        con$HDEL(keys$tasks_expr,     id)
        con$HDEL(keys$tasks_status,   id)
        con$HDEL(keys$tasks_envir,    id)
        con$HDEL(keys$tasks_complete, id)
        con$HDEL(keys$tasks_group,    id)
        con$HDEL(keys$tasks_result,   id)
      })

      ret
    },

    tasks_set_group=function(task_ids, group, exists_action="stop") {
      tasks_set_group(self$con, self$keys, task_ids, group, exists_action)
    }
  ))

##' Create an rrqueue queue
##' @title Create an rrqueue queue
##' @param queue_name Queue name
##' @param packages Character vector of packages to load
##' @param sources Character vector of files to source
##' @param redis_host Redis hostname
##' @param redis_port Redis port number
##' @param global Source files into the global environment?  This is a
##'   good idea for working with the "user friendly" functions.  See
##'   issue 2 on github.
##' @export
queue <- function(queue_name, packages=NULL, sources=NULL,
                  redis_host="127.0.0.1", redis_port=6379, global=TRUE) {
  .R6_queue$new(queue_name, packages, sources, redis_host, redis_port, global)
}

queue_clean <- function(con, queue_name, purge=FALSE) {
  keys <- rrqueue_keys(queue_name)
  con$SREM(keys$rrqueue_queues, keys$queue_name)

  if (purge) {
    ## TODO: replace with scan_find
    del <- as.character(con$KEYS(paste0(queue_name, "*")))
    if (length(del) > 0L) {
      con$DEL(del)
    }
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

    con$DEL(keys$tasks_counter)
    con$DEL(keys$tasks_id)
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
