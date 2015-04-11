## Heartbeat support, but with a different interface to RedisHeartbeat
heartbeat <- function(key, timeout, expire, con) {
  if (requireNamespace("RedisHeartbeat", quietly=TRUE)) {
    h <- RedisHeartbeat::heartbeat(con$context$host, con$context$port)
    h$start(key, timeout, expire)
    h
  } else {
    con$SET(key, "NO_HEARTBEAT_SUPPORT")
    list(stop=function() {})
  }
}

heartbeat_time <- function(obj) {
  status <- obj$tasks_status()
  task_ids <- names(status[status == TASK_RUNNING])
  if (length(task_ids) > 0L) {
    w_running <- as.character(obj$con$HMGET(obj$keys$tasks_worker, task_ids))
    key <- rrqueue_key_worker_heartbeat(obj$name, w_running)
    d <- data.frame(worker=w_running,
                    task_id=task_ids,
                    time=vnapply(key, obj$con$PTTL),
                    stringsAsFactors=FALSE)
    rownames(d) <- NULL
  } else {
    d <- data.frame(worker=character(0),
                    task_id=character(0),
                    time=numeric(0),
                    stringsAsFactors=FALSE)
  }
  d
}

orphan_jobs <- function(obj) {
  d <- heartbeat_time(obj)
  i <- d$time == -2
  setNames(d$task_id[i], d$worker[i])
}
