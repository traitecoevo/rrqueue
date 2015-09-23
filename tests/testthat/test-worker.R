context("worker")

test_that("config", {
  expect_that(rrqueue_worker_args(character(0)),
              throws_error())

  queue_name <- "myqueue"

  opts <- rrqueue_worker_args(queue_name)
  expect_that(opts$queue_name, equals(queue_name))

  opts <- rrqueue_worker_args(c("--config", "config.yml"))
  dat <- yaml_read("config.yml")
  expect_that(opts$queue_name, equals(dat$queue_name))
  expect_that(opts$redis_host, equals(dat$redis_host))
  expect_that(opts$redis_port, equals(dat$redis_port))
  expect_that(opts$heartbeat_period, equals(dat$heartbeat_period))
  expect_that(opts$heartbeat_expire, equals(dat$heartbeat_expire))
  expect_that(opts$key_worker_alive, is_null())

  ## override some opts:
  opts <- rrqueue_worker_args(c("--config", "config.yml",
                                "--key-worker-alive", "mykey"))
  expect_that(opts$key_worker_alive, equals("mykey"))

  opts <- rrqueue_worker_args(c("--config", "config.yml",
                                "--redis-port", "9999"))
  expect_that(opts$redis_port, equals("9999"))

  opts <- rrqueue_worker_args(c("--config", "config.yml", queue_name))
  expect_that(opts$queue_name, equals(queue_name))

  ## And again with a configuration that loads very little:
  opts <- rrqueue_worker_args(c("--config", "config2.yml",
                                "--key-worker-alive", "mykey"))
  expect_that(opts$key_worker_alive, equals("mykey"))
  expect_that(opts$redis_host, equals(yaml_read("config2.yml")$redis_host))
  expect_that(opts$redis_port, equals("6379"))

  expect_that(rrqueue_worker_args(c("--config", "config3.yml")),
              throws_error("queue name must be given"))
  opts <- rrqueue_worker_args(c("--config", "config3.yml", queue_name))
  expect_that(opts$queue_name, equals(queue_name))
})

test_that("workers_times - nonexistant worker", {
  obs <- observer("tmpjobs")
  name <- "no such worker"
  t <- obs$workers_times(name)
  expect_that(t, is_a("data.frame"))
  expect_that(nrow(t), equals(1))
  expect_that(t, equals(data.frame(worker_id=name,
                                   expire_max=NA_real_,
                                   expire=-2.0,
                                   last_seen=NA_real_,
                                   last_action=NA_real_,
                                   stringsAsFactors=FALSE)))
})

test_that("workers_times - no workers", {
  obs <- observer("tmpjobs")
  t <- obs$workers_times()
  expect_that(t, is_a("data.frame"))
  expect_that(nrow(t), equals(0))
  expect_that(t, equals(data.frame(worker_id=character(0),
                                   expire_max=numeric(0),
                                   expire=numeric(0),
                                   last_seen=numeric(0),
                                   last_action=numeric(0),
                                   stringsAsFactors=FALSE)))

  expect_that(obs$workers_list(), equals(character(0)))
  expect_that(obs$workers_status(), equals(empty_named_character()))

  log <- obs$workers_log_tail()
  expect_that(log, equals(data.frame(worker_id=character(0),
                                     time=character(0),
                                     command=character(0),
                                     message=character(0),
                                     stringsAsFactors=FALSE)))
})
