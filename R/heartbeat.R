## Heartbeat support, but with a slightly different interface to
## RedisHeartbeat and falling back on something informative if we have
## no support.
heartbeat <- function(con, key, period, expire) {
  heartbeatr::heartbeat(key, period,
                            expire=expire, value=expire,
                            con$config())
}

heartbeat_time <- function(obj) {
  status <- obj$tasks_status()
  task_ids <- names(status[status == TASK_RUNNING])
  if (length(task_ids) > 0L) {
    w_running <- as.character(obj$con$HMGET(obj$keys$tasks_worker, task_ids))
    key <- rrqueue_key_worker_heartbeat(obj$queue_name, w_running)
    d <- data.frame(worker_id=w_running,
                    task_id=task_ids,
                    time=vnapply(key, obj$con$PTTL),
                    stringsAsFactors=FALSE)
    rownames(d) <- NULL
  } else {
    d <- data.frame(worker_id=character(0),
                    task_id=character(0),
                    time=numeric(0),
                    stringsAsFactors=FALSE)
  }
  d
}

identify_orphan_tasks <- function(obj) {
  d <- heartbeat_time(obj)
  i <- d$time == -2
  task_id   <- d$task_id[i]
  worker_id <- d$worker_id[i]

  con <- obj$con
  keys <- obj$keys
  time <- redis_time(obj$con)
  for (i in seq_along(task_id)) {
    con$HSET(keys$tasks_time_end, task_id[[i]],   time)
    con$HSET(keys$tasks_status,   task_id[[i]],   TASK_ORPHAN)
    con$HSET(keys$workers_status, worker_id[[i]], WORKER_LOST)
  }

  setNames(task_id, worker_id)
}
