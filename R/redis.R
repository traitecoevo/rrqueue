##' @import RedisAPI
redis_connection <- function(con, port=6379) {
  if (inherits(con, "redis_api")) {
    con
  } else if (is.null(con)) {
    RedisAPI::hiredis("127.0.0.1", port)
  } else if (is.character(con)) {
    RedisAPI::hiredis(con, port)
  } else {
    stop("Cannot create a Redis connection from object")
  }
}
