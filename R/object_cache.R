##' @importFrom storr storr
object_cache <- function(prefix, con) {
  con <- redis_connection(con)
  dr <- storr::driver_redis(prefix, con$host, con$port)
  storr::storr(dr)
}

file_cache <- function(prefix, con) {
  con <- redis_connection(con)
  dr <- storr::driver_redis(prefix, con$host, con$port)
  storr::storr(dr)
}
