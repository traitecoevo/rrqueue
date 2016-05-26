context("worker")

test_that("config", {
  expect_error(rrqueue_worker_args(character(0)),
               "usage: rrqueue_worker")

  queue_name <- "tmpjobs"

  opts <- rrqueue_worker_args(queue_name)
  expect_equal(opts$queue_name, queue_name)
  expect_equal(opts$redis_host, "127.0.0.1")
  expect_equal(opts$redis_port, 6379)
  expect_equal(opts$heartbeat_period, 30)
  expect_equal(opts$heartbeat_expire, 90)
  expect_null(opts$key_worker_alive)

  opts <- rrqueue_worker_args(c("--config", "config.yml"))
  dat <- yaml_read("config.yml")
  expect_equal(opts$queue_name, dat$queue_name)
  expect_equal(opts$redis_host, dat$redis$host)
  expect_equal(opts$redis_port, dat$redis$port)
  expect_equal(opts$heartbeat_period, dat$heartbeat_period)
  expect_equal(opts$heartbeat_expire, dat$heartbeat_expire)
  expect_null(opts$key_worker_alive)

  ## override some opts:
  opts <- rrqueue_worker_args(c("--config", "config.yml",
                                "--key-worker-alive", "mykey"))
  expect_equal(opts$key_worker_alive, "mykey")

  opts <- rrqueue_worker_args(c("--config", "config.yml",
                                "--redis-port", "9999"))
  expect_equal(opts$redis_port, "9999")

  opts <- rrqueue_worker_args(c("--config", "config.yml", queue_name))
  expect_equal(opts$queue_name, queue_name)

  ## And again with a configuration that loads very little:
  opts <- rrqueue_worker_args(c("--config", "config2.yml",
                                "--key-worker-alive", "mykey"))
  expect_equal(opts$key_worker_alive, "mykey")
  expect_equal(opts$redis_host, yaml_read("config2.yml")$redis$host)
  expect_equal(opts$redis_port, 6379)

  expect_error(rrqueue_worker_args(c("--config", "config3.yml")),
               "queue name must be given")
  opts <- rrqueue_worker_args(c("--config", "config3.yml", queue_name))
  expect_equal(opts$queue_name, queue_name)
})

test_that("workers_times - nonexistant worker", {
  obs <- observer("tmpjobs")
  name <- "no such worker"
  t <- obs$workers_times(name)
  expect_is(t, "data.frame")
  expect_equal(nrow(t), 1)
  expect_equal(t, data.frame(worker_id=name,
                             expire_max=NA_real_,
                             expire=-2.0,
                             last_seen=NA_real_,
                             last_action=NA_real_,
                             stringsAsFactors=FALSE))
})

test_that("workers_times - no workers", {
  obs <- observer("tmpjobs")
  t <- obs$workers_times()
  expect_is(t, "data.frame")
  expect_equal(nrow(t), 0)
  expect_equal(t, data.frame(worker_id=character(0),
                             expire_max=numeric(0),
                             expire=numeric(0),
                             last_seen=numeric(0),
                             last_action=numeric(0),
                             stringsAsFactors=FALSE))

  expect_equal(obs$workers_list(), character(0))
  expect_equal(obs$workers_status(), empty_named_character())

  log <- obs$workers_log_tail()
  expect_equal(log, data.frame(worker_id=character(0),
                               time=character(0),
                               command=character(0),
                               message=character(0),
                               stringsAsFactors=FALSE))
  test_cleanup()
})
