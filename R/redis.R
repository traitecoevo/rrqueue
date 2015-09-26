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

## Similar to the above, listen on a bunch of hash fields for
## something to exist.
poll_hash_keys <- function(con, keys, field, wait, every=0.05) {
  if (wait <= 0) {
    res <- lapply(keys, con$HGET, field)
  } else {
    timeout <- as.difftime(wait, units="secs")
    t0 <- Sys.time()
    ok <- logical(length(keys))
    res <- vector("list", length(keys))
    while (Sys.time() - t0 < timeout) {
      exists <- as.logical(vnapply(keys[!ok], con$HEXISTS, field))
      if (any(exists)) {
        i <- which(!ok)[exists]
        res[i] <- lapply(keys[i], con$HGET, field)
        ok[i] <- TRUE
        if (all(ok)) {
          break
        }
      }
      ## This should not be called on the last way through...
      Sys.sleep(every)
    }
  }
  names(res) <- keys
  res
}

clean_pttl <- function(x) {
  i <- x > 0
  x[i] <- x[i] / 1000
  x
}
