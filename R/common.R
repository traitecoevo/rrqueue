rrqueue_keys <- function(queue_name=NULL, worker_name=NULL) {
  if (is.null(queue_name)) {
    rrqueue_keys_global()
  } else if (is.null(worker_name)) {
    c(rrqueue_keys_global(),
      rrqueue_keys_queue(queue_name))
  } else {
    c(rrqueue_keys_global(),
      rrqueue_keys_queue(queue_name),
      rrqueue_keys_worker(queue_name, worker_name))
  }
}

rrqueue_keys_global <- function() {
  list(rrqueue_queues  = "rrqueue:queues")
}

rrqueue_keys_queue <- function(queue) {
  list(workers_name    = sprintf("%s:workers:name",    queue),
       workers_status  = sprintf("%s:workers:status",  queue),
       workers_task    = sprintf("%s:workers:task",    queue),
       workers_new     = sprintf("%s:workers:new",     queue),

       tasks_counter   = sprintf("%s:tasks:counter",   queue),
       tasks_id        = sprintf("%s:tasks:id",        queue),
       tasks_expr      = sprintf("%s:tasks:expr",      queue),
       tasks_status    = sprintf("%s:tasks:status",    queue),
       tasks_time_sub  = sprintf("%s:tasks:time:sub",  queue),
       tasks_time_beg  = sprintf("%s:tasks:time:beg",  queue),
       tasks_time_end  = sprintf("%s:tasks:time:end",  queue),
       tasks_worker    = sprintf("%s:tasks:worker",    queue),
       tasks_result    = sprintf("%s:tasks:result",    queue),
       tasks_envir     = sprintf("%s:tasks:envir",     queue),
       tasks_complete  = sprintf("%s:tasks:complete",  queue),
       tasks_redirect  = sprintf("%s:tasks:redirect",  queue),

       envirs_contents = sprintf("%s:envirs:contents", queue),

       ## Objects:
       objects         = sprintf("%s:objects",         queue))
}

rrqueue_keys_worker <- function(queue, worker) {
  list(message   = rrqueue_key_worker_message(queue, worker),
       log       = rrqueue_key_worker_log(queue, worker),
       heartbeat = rrqueue_key_worker_heartbeat(queue, worker))
}

## Special key for worker-specific commands to be published to.
rrqueue_key_worker_message <- function(queue, worker) {
  sprintf("%s:workers:%s:message", queue, worker)
}
rrqueue_key_worker_log <- function(queue, worker) {
  sprintf("%s:workers:%s:log", queue, worker)
}
rrqueue_key_worker_heartbeat <- function(queue, worker) {
  sprintf("%s:workers:%s:heartbeat", queue, worker)
}
rrqueue_key_task_complete <- function(queue, id) {
  sprintf("%s:tasks:%s:complete", queue, id)
}

## TODO: come up with a way of scheduling object deletion.  Things
## that are created here should be deleted immediately after the
## function ends (perhaps on exit).  *Objects* should only be deleted
## if they have no more dangling pointers.
##
## So we'll register "groups" and schedule prefix deletion once the
## group is done.  But for now, don't do any of that.
prepare_expression <- function(expr) {
  fun <- expr[[1]]
  args <- expr[-1]

  ## TODO: disallow *language* as arguments; instead I guess we'll
  ## serialise those as anonymous symbols, e.g. `.1:<anon1>`, and
  ## substitute back in on the other side.
  ##
  ## is_language <- vlapply(args, is.language)
  ## if (any(is_language)) {
  ##   stop("not yet supported")
  ## }
  is_symbol <- vlapply(args, is.symbol)
  if (any(is_symbol)) {
    object_names <- vcapply(args[is_symbol], as.character)
  } else {
    object_names <- NULL
  }

  list(expr=expr, object_names=object_names)
}

save_expression <- function(dat, task_id, envir, object_cache) {
  object_names <- dat$object_names
  if (!is.null(object_names)) {
    if (!all(ok <- exists(object_names, envir, inherits=FALSE))) {
      stop("not all objects found: ",
           paste(object_names[!ok], collapse=", "))
    }
    object_names_to <- paste0(task_object_prefix(task_id), object_names)
    for (i in seq_along(object_names)) {
      object_cache$set(object_names_to[[i]],
                       get(object_names[[i]], envir, inherits=FALSE),
                       store_in_envir=FALSE)
    }
    names(dat$object_names) <- object_names_to
  }

  object_to_string(dat)
}

restore_expression <- function(dat, envir, object_cache) {
  dat <- string_to_object(dat)
  object_names <- dat$object_names
  if (length(object_names) > 0L) {
    object_names_to <- names(object_names)
    for (i in seq_along(dat$object_names)) {
      assign(object_names[[i]],
             object_cache$get(object_names_to[[i]]),
             envir=envir)
    }
  }
  dat$expr
}

parse_worker_name <- function(str) {
  res <- strsplit(str, "::", fixed=TRUE)
  if (any(viapply(res, length) != 2)) {
    stop("parse error")
  }
  list(host=vcapply(res, "[[", 1),
       pid=vcapply(res, "[[", 2))
}

parse_worker_log <- function(log) {
  re <- "^([0-9]+) ([^ ]+) ?(.*)$"
  ok <- grepl(re, log)
  if (!all(ok)) {
    stop("Corrupt log")
  }
  time <- as.integer(sub(re, "\\1", log))
  command <- sub(re, "\\2", log)
  message <- lstrip(sub(re, "\\3", log))
  data.frame(time, command, message, stringsAsFactors=FALSE)
}

task_object_prefix <- function(task_id) {
  sprintf(".%s:", task_id)
}
