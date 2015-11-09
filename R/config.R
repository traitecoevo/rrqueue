load_config <- function(filename) {
  keys_common <- c("queue_name")
  keys_worker <- c("heartbeat_period", "heartbeat_expire", "key_worker_alive")
  keys_queue  <- c("packages", "sources")

  ## Note that this is *not* the defaults to the underlying worker
  ## functions; I'll rework the docopt to read those soon.
  defaults_worker <- as.list(formals(worker))[c(keys_common, keys_worker)]
  defaults_queue  <- as.list(formals(queue))[c(keys_common, keys_queue)]
  if (!isTRUE(all.equal(defaults_worker[keys_common],
                        defaults_queue[keys_common]))) {
    stop("This is a bug.")
  }
  defaults <- c(defaults_worker, defaults_queue[keys_queue])
  defaults$queue_name <- NULL
  defaults$redis <- list()

  config <- yaml_read(filename)

  extra <- setdiff(names(config),
                   c(keys_common, keys_worker, keys_queue, "redis"))
  if (length(extra) > 0L) {
    warning(sprintf("Unknown keys in %s: %s",
                    filename, paste(extra, collapse=", ")))
  }

  ## Some validation:
  assert_character_or_null(config$packages)
  assert_character_or_null(config$sources)

  ret <- modifyList(defaults, config, keep.null=TRUE)
}

## This needs fixing in a few places though I don't think all are
## tested.  Eventually we move away from host/port pairs and go with
## redis_config.  Once that happens the interface here is heaps easier
## and basically can go away.
##
## Testing like in test-worker will be needed for observer/queue which
## also use this.
tmp_fix_redis_config <- function(cfg) {
  if (!is.null(cfg$redis)) {
    cfg[paste0("redis_", names(cfg$redis))] <- cfg$redis
    cfg$redis <- NULL
  }
  cfg
}
