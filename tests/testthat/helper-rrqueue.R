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
  test_queue_clean("testq:heartbeat")
}

test_queue_clean <- function(name) {
  queue_clean(redis_connection(NULL), name, purge=TRUE,
              stop_workers=TRUE, kill_local_workers=TRUE, wait_stop=0.05)
}

skip_if_no_heartbeat <- function() {
  if (heartbeat_available()) {
    return()
  }
  skip("RedisHeartbeat is not available")
}

## Looks like a bug to me, relative to the docs:
PSKILL_SUCCESS <- tools::pskill(Sys.getpid(), 0)
pid_exists <- function(pid) {
  tools::pskill(pid, 0) == PSKILL_SUCCESS
}

wait_until_hash_field_exists <- function(con, key, field, every=.05,
                                         timeout=as.difftime(5, units="secs")) {
  t0 <- Sys.time()
  while (Sys.time() - t0 < timeout) {
    if (con$HEXISTS(key, field)) {
      return()
    }
    Sys.sleep(every)
  }
  stop(sprintf("field '%s' did not appear in time", field))
}

with_wd <- function(path, expr) {
  owd <- setwd(path)
  on.exit(setwd(owd))
  force(expr)
}
