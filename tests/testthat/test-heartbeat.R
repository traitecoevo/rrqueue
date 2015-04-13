context("heartbeat")

test_that("heartbeat", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_that(existing, equals(character(0)))

  ## TODO: probably best to use different queue names for different
  ## tests?
  obj <- queue("testq:heartbeat", sources="myfuns.R")

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- rrqueue_worker_spawn(obj$name, logfile,
                              heartbeat_period=1, heartbeat_expire=3)
  ## w <- rrqueue::worker("testq:heartbeat")
  expect_that(obj$n_workers(), equals(1))

  ## First, check that things are working
  t <- obj$enqueue(sin(1))
  done <- obj$con$BLPOP(t$key_complete, 10)
  expect_that(t$result(), equals(sin(1)))
  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE)))

  t_double <- 5
  e <- environment()
  t <- obj$enqueue(slowdouble(t_double), e)
  Sys.sleep(0.5)
  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_RUNNING)))

  ## Then, test that we see a heartbeat
  h <- rrqueue_key_worker_heartbeat(obj$name, wid)
  expect_that(obj$con$GET(h), equals("OK"))
  ttl <- obj$con$TTL(h)
  ## TODO: Need to make heartbeat configurable upon spawning worker or
  ## we're going to wait for ages
  expect_that(ttl, is_more_than(1))
  expect_that(ttl, is_less_than(3 + 1))

  Sys_kill(parse_worker_name(wid)$pid)
  Sys.sleep(0.5)

  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_RUNNING)))
  ttl <- obj$con$TTL(h)
  Sys.sleep(ttl + 1)

  expect_that(obj$con$TTL(h), equals(-2))
  expect_that(heartbeat_time(obj)$time, equals(-2))

  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_RUNNING)))

  orphans <- identify_orphan_tasks(obj)
  expect_that(orphans, equals(setNames("2", wid)))

  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_ORPHAN)))
  expect_that(t$status(), equals(TASK_ORPHAN))

  t2 <- obj$requeue(t$id)
  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_REDIRECT,
                                           "3"=TASK_PENDING)))

  logfile2 <- sub("\\.log$", "2.log", logfile)
  wid2 <- rrqueue_worker_spawn(obj$name, logfile2)
  ##   w <- rrqueue::worker("testq:heartbeat")

  Sys.sleep(0.5)
  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_REDIRECT,
                                           "3"=TASK_RUNNING)))
  Sys.sleep(t_double)
  expect_that(obj$tasks_status(), equals(c("1"=TASK_COMPLETE,
                                           "2"=TASK_REDIRECT,
                                           "3"=TASK_COMPLETE)))

  t0 <- as.numeric(obj$con$HGET(obj$keys$tasks_time_0, t2$id))
  t1 <- as.numeric(obj$con$HGET(obj$keys$tasks_time_1, t2$id))
  expect_that(t1 - t0, is_more_than(t_double))
  expect_that(t1 - t0, is_less_than(t_double + 1))

  obj$send_message("STOP")
})
