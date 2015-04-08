rrqueue_keys <- function(queue) {
  list(workers        = sprintf("%s:workers",        queue),
       workers_status = sprintf("%s:workers:status", queue),
       workers_task   = sprintf("%s:workers:task",   queue),

       ## The tasks database:
       tasks          = sprintf("%s:tasks",         queue),
       tasks_counter  = sprintf("%s:tasks:counter", queue),
       tasks_id       = sprintf("%s:tasks:id",      queue),
       tasks_status   = sprintf("%s:tasks:status",  queue),
       tasks_result   = sprintf("%s:tasks:result",  queue),

       ## Environment setup:
       packages       = sprintf("%s:environment:packages", queue),
       sources        = sprintf("%s:environment:sources",  queue),
       objects        = sprintf("%s:environment:objects",  queue))
}

## Special key for worker-specific commands to be published to.
rrqueue_key_worker <- function(queue, worker) {
  ## TODO: rename -> rrqueue_key_worker_message
  sprintf("%s:worker:%s:message", queue, worker)
}
rrqueue_key_worker_log <- function(queue, worker) {
  sprintf("%s:worker:%s:log", queue, worker)
}
rrqueue_key_task_objects <- function(queue, task_id) {
  sprintf("%s:tasks:objects:%s", queue, task_id)
}

## TODO: come up with a way of scheduling object deletion.  Things
## that are created here should be deleted immediately after the
## function ends (perhaps on exit).  *Objects* should only be deleted
## if they have no more dangling pointers.
##
## So we'll register "groups" and schedule prefix deletion once the
## group is done.  But for now, don't do any of that.
save_expression <- function(expr, prefix, envir, object_cache) {
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
    name_from <- vcapply(args[is_symbol], as.character)
    name_to   <- paste0(prefix, name_from)
    args[is_symbol] <- lapply(name_to, as.symbol)
    ## Store the required objects
    for (i in seq_along(name_from)) {
      object_cache$set(name_to[[i]],
                       get(name_from[[i]], envir),
                       store_in_envir=FALSE)
    }
    names(name_from) <- name_to
  } else {
    name_from <- NULL
  }

  ret <- list(expr=expr, objects=name_from)
  ret$str <- object_to_string(ret)
  ret
}

restore_expression <- function(dat, envir, object_cache) {
  dat <- string_to_object(dat)
  objects <- dat$objects
  if (length(objects) > 0L) {
    objects_to <- names(objects)
    for (i in seq_along(dat$objects)) {
      assign(objects[[i]], object_cache$get(objects_to[[i]]), envir=envir)
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
