## Start of bits for querying the state of the system
queues <- function(redis_host=NULL, ...) {
  con <- redis_connection(redis_host, ...)
  as.character(con$SMEMBERS("rrqueue:queues"))
}
