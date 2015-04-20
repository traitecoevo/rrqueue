object_to_string <- RedisAPI::object_to_string
string_to_object <- RedisAPI::string_to_object

## TODO: run in db 15 or something?
empty_named_list <- function() {
  structure(list(), names = character(0))
}
empty_named_character <- function() {
  structure(character(0), names = character(0))
}

test_cleanup <- function() {
  queue_clean(redis_connection(NULL), "tmpjobs", TRUE)
  queue_clean(redis_connection(NULL), "testq:heartbeat", TRUE)
}

skip_if_no_heartbeat <- function() {
  if (heartbeat_available()) {
    return()
  }
  skip("RedisHeartbeat is not available")
}
