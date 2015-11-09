##' @importFrom storr storr
object_cache <- function(prefix, con) {
  dr <- storr::driver_redis(prefix, con$config())
  storr::storr(dr)
}

## TODO: weirdly this is the same thing as object_cache, but a
## different prefix.
file_cache <- function(prefix, con) {
  dr <- storr::driver_redis(prefix, con$config())
  storr::storr(dr)
}
