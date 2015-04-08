##' @importFrom rrlite hiredis
redis_connection <- function(con) {
  if (is.null(con)) rrlite::hiredis() else con
}

redis_version <- function(con) {
  rrlite::parse_redis_info(con$INFO())$redis_version
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
