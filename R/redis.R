##' @importFrom RedisAPI hiredis object_to_string string_to_object
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

from_redis_hash <- function(con, name, keys=NULL, f=as.character,
                            missing=NA_character_) {
  if (is.null(keys)) {
    x <- con$HGETALL(name)
    dim(x) <- c(2, length(x) / 2)
    setNames(f(x[2, ]),
             as.character(x[1, ]))
  } else {
    x <- con$HMGET(name, keys)
    x[vlapply(x, is.null)] <- missing
    setNames(f(x), as.character(keys))
  }
}

redis_time <- function(con) {
  paste(as.character(con$TIME()), collapse=".")
}

redis_time_to_r <- function(x) {
  as.POSIXct(as.numeric(x), origin="1970-01-01")
}
