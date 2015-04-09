rrqueue_keys <- function(queue) {
  list(rrqueue_queues  = "rrqueue:queues",

       workers_name    = sprintf("%s:workers:name",    queue),
       workers_status  = sprintf("%s:workers:status",  queue),
       workers_task    = sprintf("%s:workers:task",    queue),

       tasks_counter   = sprintf("%s:tasks:counter",   queue),
       tasks_id        = sprintf("%s:tasks:id",        queue),
       tasks_expr      = sprintf("%s:tasks:expr",      queue),
       tasks_status    = sprintf("%s:tasks:status",    queue),
       tasks_result    = sprintf("%s:tasks:result",    queue),
       tasks_envir     = sprintf("%s:tasks:envir",     queue),

       envirs_counter  = sprintf("%s:envirs:counter",  queue),
       envirs_packages = sprintf("%s:envirs:packages", queue),
       envirs_sources  = sprintf("%s:envirs:sources",  queue),
       envirs_default  = sprintf("%s:envirs:default",  queue),

       ## Objects:
       objects         = sprintf("%s:objects",         queue))
}

## Special key for worker-specific commands to be published to.
rrqueue_key_worker <- function(queue, worker) {
  ## TODO: rename -> rrqueue_key_worker_message
  sprintf("%s:worker:%s:message", queue, worker)
}
rrqueue_key_worker_log <- function(queue, worker) {
  sprintf("%s:worker:%s:log", queue, worker)
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
