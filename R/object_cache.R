## Content-addressable get/set.  Could be generally useful.
##
## Another way of running this would be to not do the data expiry and
## keep things around until this dies.
.R6_object_cache <- R6::R6Class(
  "object_cache",

  public=list(
    con=NULL,
    keys=NULL,
    envir=NULL,

    initialize=function(con, key) {
      self$con   <- redis_connection(con)
      self$keys  <- object_cache_keys(key)
      self$envir <- new.env(parent=emptyenv())
    },

    list=function(con) {
      as.character(self$con$hkeys(self$keys$hash))
    },

    drop=function(names) {
      con <- self$con
      keys <- self$keys
      hashes <- as.character(con$HMGET(keys$hash, names))
      con$HDEL(keys$hash, names)
      for (h in hashes) {
        n <- con$HINCRBY(keys$count, h, -1)
        if (n <= 0L) {
          con$HDEL(keys$count, h)
          con$HDEL(keys$data,  h)
          rm(list=h, envir=self$envir)
        }
      }
    },

    set=function(name, value, store_in_envir=TRUE) {
      str <- object_to_string(value)
      hash <- hash_string(str)
      self$con$HSET(self$keys$hash, name, hash)
      self$con$HINCRBY(self$keys$count, hash, 1)
      if (self$con$HEXISTS(self$keys$data, hash) == 0L) {
        self$con$HSET(self$keys$data, hash, str)
        ## this must also not exist then.
        if (store_in_envir) {
          assign(hash, value, self$envir)
        }
      }
    },

    get=function(name) {
      hash <- self$con$HGET(self$keys$hash, name)
      if (is.null(hash)) {
        stop("object not found")
      }
      if (exists(hash, self$envir, inherits=FALSE)) {
        get(hash, self$envir, inherits=FALSE)
      } else {
        ret <- string_to_object(self$con$HGET(self$keys$data, hash))
        assign(hash, ret, self$envir)
        ret
      }
    }))

object_cache_keys <- function(key) {
  list(data  = paste0(key, ":data"),
       hash  = paste0(key, ":hash"),
       count =paste0(key, ":count"))
}

object_cache <- function(con, key) {
  .R6_object_cache$new(con, key)
}

object_cache_cleanup <- function(con, key) {
  con$DEL(as.character(object_cache_keys(key)))
}
