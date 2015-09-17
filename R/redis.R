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

## Much of this file wants to move into RedisAPI I think.  It's all
## internal so is very easy to move *so long* was I Import all of
## RedisAPI.  It really wants testing anyway so that's for the best.
redis_version <- function(con) {
  RedisAPI::parse_info(con$INFO())$redis_version
}

redis_multi <- function(con, code) {
  ## TODO: This is better done with tryCatch / finally
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
    ## NOTE: This is needed for the case where missing=NULL, otherwise
    ## it will *delete* the elements.  However, if missing is NULL,
    ## then f should really be a list-returning function otherwise
    ## NULL -> "NULL.
    if (!is.null(missing)) {
      x[vlapply(x, is.null)] <- missing
    }
    setNames(f(x), as.character(keys))
  }
}

redis_time <- function(con) {
  format_redis_time(con$TIME())
}

format_redis_time <- function(x) {
  paste(as.character(x), collapse=".")
}

redis_time_to_r <- function(x) {
  as.POSIXct(as.numeric(x), origin="1970-01-01")
}
