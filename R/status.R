## Start of bits for querying the state of the system
##
## TODO: This becomes list_queues(), for consistency with the rest of
## the system.
queues <- function(redis_host=NULL, redis_port=6379) {
  con <- redis_connection(redis_host, redis_port)
  as.character(con$SMEMBERS("rrqueue:queues"))
}
