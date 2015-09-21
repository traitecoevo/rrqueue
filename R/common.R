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
  list(queue_name      = queue,

       workers_name    = sprintf("%s:workers:name",    queue),
       workers_status  = sprintf("%s:workers:status",  queue),
       workers_task    = sprintf("%s:workers:task",    queue),
       workers_new     = sprintf("%s:workers:new",     queue),
       workers_info    = sprintf("%s:workers:info",    queue),

       tasks_counter   = sprintf("%s:tasks:counter",   queue),
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
       tasks_group     = sprintf("%s:tasks:group",     queue),

       envirs_contents = sprintf("%s:envirs:contents", queue),
       envirs_files    = sprintf("%s:envirs:files",    queue),

       files           = sprintf("%s:files",           queue),
       objects         = sprintf("%s:objects",         queue))
}

## NOTE: Or alternatively, key_tasks?
rrqueue_key_queue <- function(queue, envir) {
  sprintf("%s:tasks:%s:id", queue, envir)
}

rrqueue_keys_worker <- function(queue, worker) {
  list(message   = rrqueue_key_worker_message(queue, worker),
       response  = rrqueue_key_worker_response(queue, worker),
       log       = rrqueue_key_worker_log(queue, worker),
       heartbeat = rrqueue_key_worker_heartbeat(queue, worker),
       envir     = rrqueue_key_worker_envir(queue, worker))
}

## Special key for worker-specific commands to be published to.
rrqueue_key_worker_message <- function(queue, worker) {
  sprintf("%s:workers:%s:message", queue, worker)
}
rrqueue_key_worker_response <- function(queue, worker) {
  sprintf("%s:workers:%s:response", queue, worker)
}
rrqueue_key_worker_log <- function(queue, worker) {
  sprintf("%s:workers:%s:log", queue, worker)
}
rrqueue_key_worker_heartbeat <- function(queue, worker) {
  sprintf("%s:workers:%s:heartbeat", queue, worker)
}
rrqueue_key_worker_envir <- function(queue, worker) {
  sprintf("%s:workers:%s:envir", queue, worker)
}
rrqueue_key_task_complete <- function(queue, task_id) {
  sprintf("%s:tasks:%s:complete", queue, task_id)
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
    names(object_names) <- paste0(task_object_prefix(task_id), object_names)
    dat$object_names <- object_names

    object_cache$import(envir, object_names)
  }

  object_to_string(dat)
}

restore_expression <- function(dat, envir, object_cache) {
  dat <- string_to_object(dat)
  if (!is.null(object_cache) && length(dat$object_names) > 0L) {
    object_cache$export(envir, invert_names(dat$object_names))
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

version_info <- function(package=.packageName) {
  descr <- packageDescription(package)
  version <- package_version(descr$Version)
  repository <- descr$Repository
  sha <- descr$RemoteSha
  list(package=package,
       version=version,
       repository=repository,
       sha=sha)
}

version_string <- function() {
  data <- version_info()
  if (!is.null(data$repository)) {
    qual <- data$repository
  } else if (!is.null(data$sha)) {
    qual <- data$sha
  } else {
    qual <- "LOCAL"
  }
  sprintf("%s [%s]", data$version, qual)
}

rrqueue_scripts <- function(con) {
  set_hashes <- 'local id = ARGV[table.getn(ARGV)]
for i, k in ipairs(KEYS) do
redis.call("HSET", k, id, ARGV[i])
end'

  ## Assume that ARGV[1] is the task id and KEYS[1] is the queue.  Then
  ## ARGV[2..] and KEYS[2..] are the key / value pairs
  job_submit <- '
local task_id = ARGV[1]
for i, k in ipairs(KEYS) do
  if i > 1 then
    redis.call("HSET", k, task_id, ARGV[i])
  end
end
redis.call("RPUSH", KEYS[1], task_id)'

  job_incr <- 'return {redis.call("INCR", KEYS[1]), redis.call("TIME")}'
  RedisAPI::redis_scripts(con,
                          scripts=list(set_hashes=set_hashes,
                                       job_incr=job_incr,
                                       job_submit=job_submit))
}

message_prepare <- function(id, command, args) {
  object_to_string(list(id=id, command=command, args=args))
}
response_prepare <- function(id, command, result) {
  object_to_string(list(id=id, command=command, result=result))
}
