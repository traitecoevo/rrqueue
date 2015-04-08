## Content-addressable get/set.  Could be generally useful.
.R6_object_cache <- R6::R6Class(
  "object_cache",

  public=list(
    con=NULL,
    key=NULL,
    key_hash=NULL,
    cache=NULL,

    initialize=function(con, key) {
      self$con      <- redis_connection(con)
      self$key      <- key
      self$key_hash <- paste0(key, ":hash")
      self$cache    <- new.env(parent=emptyenv())
    },

    list=function(con) {
      as.character(self$con$hkeys(self$key_hash))
    },

    set=function(name, value) {
      str <- object_to_string(value)
      hash <- hash_string(str)
      self$con$HSET(self$key_hash, name, hash)
      self$con$HSET(self$key,      hash, str)
      assign(hash, value, self$cache)
    },

    get=function(name) {
      hash <- self$con$HGET(self$key_hash, name)
      if (exists(hash, self$cache, inherits=FALSE)) {
        get(hash, self$cache, inherits=FALSE)
      } else {
        ret <- string_to_object(self$con$HGET(self$key, hash))
        assign(hash, ret, self$cache)
        ret
      }
    }))

object_cache <- function(con, key) {
  .R6_object_cache$new(con, key)
}

object_cache_cleanup <- function(con, key) {
  con$DEL(c(key, paste0(key, ":hash")))
}
