load_config <- function(filename) {
  keys_common <- c("queue_name", "redis_host", "redis_port")
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

  config <- yaml_read(filename)

  extra <- setdiff(names(config), c(keys_common, keys_worker, keys_queue))
  if (length(extra) > 0L) {
    warning(sprintf("Unknown keys in %s: %s",
                    filename, paste(extra, collapse=", ")))
  }

  ## Some validation:
  assert_character_or_null(config$packages)
  assert_character_or_null(config$sources)

  modifyList(defaults, config, keep.null=TRUE)
}
