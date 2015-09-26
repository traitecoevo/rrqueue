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
  test_queue_clean("tmpjobs")
  test_queue_clean("myqueue") # vignette
  test_queue_clean("testq:heartbeat")
}

test_queue_clean <- function(name) {
  queue_clean(redis_connection(NULL), name, purge=TRUE,
              stop_workers=TRUE, kill_local=TRUE, wait_stop=0.05)
}

## Looks like a bug to me, relative to the docs:
PSKILL_SUCCESS <- tools::pskill(Sys.getpid(), 0)
pid_exists <- function(pid) {
  tools::pskill(pid, 0) == PSKILL_SUCCESS
}
