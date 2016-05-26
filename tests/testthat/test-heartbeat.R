context("heartbeat")

## This file collects all the really nasty stuff with starting and
## destroying worker processes, and detecting that with the heartbeat
## process.  There is a lot of mocking up and it's slow to develop and
## test.  There is a real problem with cascading test failures
## throughout, so many failures might just be caused by a single root
## thing.
test_that("heartbeat with signals", {
  test_cleanup()

  ## Fairly fast worker timeout:
  expire <- 3
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)

  pid <- obj$workers_info()[[wid]]$pid
  expect_true(pid_exists(pid))

  expect_equal(obj$workers_running(wid),
               setNames(TRUE, wid))
  expect_equal(obj$workers_status(),
               setNames(WORKER_IDLE, wid))

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  expect_equal(obj$workers_status(wid), setNames("BUSY", wid))
  expect_true(is.na(t$times()[["finished"]]))

  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  expect_equal(t$status(), TASK_ORPHAN)
  expect_false(is.na(t$times()[["finished"]]))

  ## Machine still alive:
  expect_true(pid_exists(pid))

  ## Realistically, this will trigger fairly quickly.
  for (i in seq_len(10)) {
    ## Another one at this point will do nothing:
    obj$send_signal(tools::SIGINT, wid)
    Sys.sleep(.5)
    expect_true(pid_exists(pid))

    ## Check that an additional job will work OK and not get caught by
    ## the *second* interrupt (this did happen to me before)
    tt <- obj$enqueue(sin(1))
    expect_silent(res <- tt$wait(1))
    expect_equal(res, sin(1))

    if ("REQUEUE" %in% obj$workers_log_tail(wid, 0)$command) {
      break
    }
  }
  expect_true("REQUEUE" %in% obj$workers_log_tail(wid, 0)$command)

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  obj$send_signal(tools::SIGTERM, wid)
  Sys.sleep(.5)

  expect_false(pid_exists(pid))

  expect_equal(obj$workers_running(wid),
               setNames(TRUE, wid))
  expect_equal(obj$workers_list(), wid)

  Sys.sleep(expire)

  expect_equal(obj$workers_running(wid),
               setNames(FALSE, wid))
  ## Still in the workers list, though:
  expect_equal(obj$workers_list(), wid)

  ret <- obj$workers_identify_lost()

  expect_equal(ret, list(workers=wid, tasks=t$id))
  expect_equal(t$status(), TASK_ORPHAN)
  expect_false(is.na(t$times()[["finished"]]))
  expect_equal(obj$workers_status(wid), setNames(WORKER_LOST, wid))
  expect_equal(obj$workers_list_exited(), wid)

  ## We were killed in the act:
  log <- obj$workers_log_tail(wid, 1)
  expect_equal(log$command, "TASK_START")

  ## Then we can clean this mess up:
  res <- obj$workers_delete_exited()
  expect_equal(res, wid)
  expect_equal(obj$workers_list_exited(), character(0))
  expect_equal(obj$workers_delete_exited(), character(0))

  ## No worker keys, plenty of task keys:
  expect_gt(length(redux::scan_find(obj$con, "tmpjobs:tasks:*")), 0)
  expect_equal(redux::scan_find(obj$con, "tmpjobs:workers:*"), character(0))
})

test_that("heartbeat shutdown when running job", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_true(pid_exists(pid))

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  expect_equal(obj$workers_status(wid), setNames("BUSY", wid))

  ## This message will be ignored:
  obj$send_message("STOP")
  Sys.sleep(.5)
  expect_equal(obj$workers_status(wid), setNames("BUSY", wid))
  expect_true(pid_exists(pid))
  expect_equal(t$status(), "RUNNING")

  ## But after a message will be honoured:
  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  ## The shutdown *is* clean:
  expect_equal(obj$workers_status(wid),
               setNames(WORKER_EXITED, wid))
  log <- obj$workers_log_tail(wid, 1)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "OK")
  expect_false(pid_exists(pid))

  ## The task was orphaned:
  expect_equal(t$status(), TASK_ORPHAN)
})

test_that("requeue orphaned jobs", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_true(pid_exists(pid))

  t_double <- 5
  t <- obj$enqueue(slowdouble(t_double))
  Sys.sleep(.5)
  expect_equal(obj$workers_status(wid), setNames("BUSY", wid))
  expect_true(is.na(t$times()[["finished"]]))

  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  expect_equal(t$status(), TASK_ORPHAN)
  expect_false(is.na(t$times()[["finished"]]))

  t2 <- obj$requeue(t$id)
  expect_equal(t$status(), TASK_REDIRECT)
  Sys.sleep(0.5)
  expect_equal(t2$status(), TASK_RUNNING)
  Sys.sleep(t_double)
  expect_equal(t2$status(), TASK_COMPLETE)

  expect_equal(t$status(follow_redirect=TRUE), TASK_COMPLETE)
  expect_equal(t$result(follow_redirect=TRUE), t_double * 2)
  expect_error(t$result(), "task [0-9]+ is unfetchable")

  tt <- obj$tasks_times(t2$id)
  expect_is(tt, "data.frame")
  expect_gt(tt$running, t_double - 1)
  expect_lt(tt$running, t_double + 1)

  obj$send_message("STOP")
})


test_that("workers stop", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_true(pid_exists(pid))

  ## TODO: worker handles could help avoid this bit of mock up:
  expect_message(msg <- worker_stop_message(
                   list(con=obj$con,
                        keys=list(queue_name=obj$queue_name),
                        name=wid,
                        styles=worker_styles())))
  cmp <- bquote(rrqueue::worker_stop(.(obj$queue_name), .(wid)))
  expect_equal(parse(text=msg[[length(msg)]])[[1]], cmp)
  eval(cmp, .GlobalEnv)

  Sys.sleep(.5)
  expect_false(pid_exists(pid))
  log <- obj$workers_log_tail(wid, 1)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "OK")
})

test_that("stop_workers (simple case)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  ok <- obj$stop_workers(wid, wait=10)
  expect_equal(ok, wid)

  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$workers_list_exited(), wid)
  log <- obj$workers_log_tail(wid, 3)
  expect_equal(log$command, c("MESSAGE", "RESPONSE", "STOP"))
  expect_equal(log$message, c("STOP", "STOP", "OK"))

  res <- obj$workers_identify_lost()
  expect_equal(res, list(workers=character(), tasks=character()))

  expect_equal(obj$workers_delete_exited(), wid)
  expect_equal(obj$workers_list_exited(), character(0))
  expect_equal(nrow(obj$workers_log_tail(wid, 1)), 0)
})

test_that("stop_workers (running, interrupt)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  len <- 2
  t <- obj$enqueue(slowdouble(len))
  Sys.sleep(.25)
  expect_equal(t$status(), TASK_RUNNING)
  ok <- obj$stop_workers(wid, interrupt=TRUE)
  expect_equal(ok, wid)

  Sys.sleep(.25)

  expect_equal(t$status(), TASK_ORPHAN)
  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$workers_list_exited(), wid)
  log <- obj$workers_log_tail(wid, 5)
  expect_equal(log$command, c("INTERRUPT", "TASK_ORPHAN",
                              "MESSAGE", "RESPONSE", "STOP"))
  expect_equal(log$message, c("", t$id, "STOP", "STOP", "OK"))

  res <- obj$workers_identify_lost()
  expect_equal(res, list(workers=character(), tasks=character()))

  expect_equal(obj$workers_delete_exited(), wid)
  expect_equal(obj$workers_list_exited(), character(0))
  expect_equal(nrow(obj$workers_log_tail(wid, 1)), 0)
})

test_that("stop_workers (running, wait)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  len <- 2
  t <- obj$enqueue(slowdouble(len))
  Sys.sleep(.25)
  expect_equal(t$status(), TASK_RUNNING)
  ok <- obj$stop_workers(wid, interrupt=FALSE)
  expect_equal(ok, wid)

  Sys.sleep(len + .25)
  expect_equal(t$status(), TASK_COMPLETE)
  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$workers_list_exited(), wid)
  log <- obj$workers_log_tail(wid, 1)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "OK")

  expect_equal(obj$workers_delete_exited(), wid)
  expect_equal(obj$workers_list_exited(), character(0))
  expect_equal(nrow(obj$workers_log_tail(wid, 1)), 0)
})

test_that("stop_workers (blocking)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  expire <- 3
  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_expire=expire, heartbeat_period=1)

  t <- obj$enqueue(block(10))
  Sys.sleep(.5)

  ok <- obj$stop_workers(wid, wait=1)
  expect_equal(ok, wid)
  expect_equal(t$status(), TASK_RUNNING)

  Sys.sleep(expire)
  res <- obj$workers_identify_lost()
  expect_equal(res, list(workers=wid, tasks=t$id))
  expect_equal(t$status(), TASK_ORPHAN)
})

test_that("clean up multiple workers", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$workers_list_exited(), character(0))

  expire <- 3
  n <- 4
  logfile <- sprintf("worker_%d.log", seq_len(n))
  wid <- worker_spawn(obj$queue_name, logfile, n=n,
                      heartbeat_expire=expire, heartbeat_period=1)

  expect_equal(sort(obj$workers_list()), sort(wid))
  for (i in seq_len(n)) {
    obj$enqueue(slowdouble(1000))
  }
  Sys.sleep(.5)
  ids <- obj$workers_task_id()

  ## Kill two savagely:
  wkill <- obj$workers_list()[2:3]
  pid <- viapply(obj$workers_info()[wkill], "[[", "pid")
  ok <- tools::pskill(pid, tools::SIGTERM) == PSKILL_SUCCESS
  Sys.sleep(expire + 1)

  expect_equal(sort(obj$workers_list()), sort(wid))
  x <- obj$workers_identify_lost()

  expect_equal(sort(x$workers), sort(wkill))
  expect_equal(sort(x$tasks), sort(unname(ids[wkill])))

  expect_equal(obj$tasks_status(x$tasks),
               setNames(c(TASK_ORPHAN, TASK_ORPHAN),
                        x$tasks))
  expect_equal(obj$workers_status(x$workers),
               setNames(c(WORKER_LOST, WORKER_LOST), wkill))

  obj$stop_workers()
})
