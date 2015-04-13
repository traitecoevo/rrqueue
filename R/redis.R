##' @importFrom RedisAPI hiredis object_to_string string_to_object
redis_connection <- function(con) {
  if (is.null(con)) RedisAPI::hiredis() else con
}

redis_version <- function(con) {
  RedisAPI::parse_info(con$INFO())$redis_version
}

redis_multi <- function(con, code) {
  con$MULTI()
  on.exit(con$EXEC())
  code
  res <- con$EXEC()
  on.exit()
  res
}

from_redis_hash <- function(con, name, f=as.character) {
  x <- con$HGETALL(name)
  dim(x) <- c(2, length(x) / 2)
  setNames(f(x[2, ]),
           as.character(x[1, ]))
}

redis_time <- function(con) {
  paste(as.character(con$TIME()), collapse=".")
}
