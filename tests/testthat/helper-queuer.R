object_to_string <- RedisAPI::object_to_string
string_to_object <- RedisAPI::string_to_object

## TODO: run in db 15 or something?
empty_named_list <- function() {
  structure(list(), names = character(0))
}
empty_named_character <- function() {
  structure(character(0), names = character(0))
}

## TODO: This will move into the package.
## TODO: should be done with a cursor.
rrqueue_cleanup <- function(con, name) {
  del <- as.character(con$KEYS(paste0(name, "*")))
  if (length(del) > 0L) {
    con$DEL(del)
  }
  con$SREM("rrqueue:queues", name)
}

test_cleanup <- function() {
  rrqueue_cleanup(redis_connection(NULL), "tmpjobs")
  rrqueue_cleanup(redis_connection(NULL), "testq:heartbeat")
}

skip_if_no_heartbeat <- function() {
  if (heartbeat_available()) {
    return()
  }
  skip("RedisHeartbeat is not available")
}
