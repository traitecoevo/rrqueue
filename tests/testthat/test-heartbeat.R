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
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)

  pid <- obj$workers_info()[[wid]]$pid
  expect_that(pid_exists(pid), is_true())

  expect_that(obj$workers_running(wid),
              equals(setNames(TRUE, wid)))
  expect_that(obj$workers_status(),
              equals(setNames(WORKER_IDLE, wid)))

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  expect_that(obj$workers_status(wid), equals(setNames("BUSY", wid)))
  expect_that(is.na(t$times()[["finished"]]), is_true())

  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  expect_that(t$status(), equals(TASK_ORPHAN))
  expect_that(is.na(t$times()[["finished"]]), is_false())

  ## Machine still alive:
  expect_that(pid_exists(pid), is_true())

  ## Realistically, this will trigger fairly quickly.
  for (i in seq_len(10)) {
    ## Another one at this point will do nothing:
    obj$send_signal(tools::SIGINT, wid)
    Sys.sleep(.5)
    expect_that(pid_exists(pid), is_true())

    ## Check that an additional job will work OK and not get caught by
    ## the *second* interrupt (this did happen to me before)
    tt <- obj$enqueue(sin(1))
    expect_that(res <- tt$wait(1), not(throws_error()))
    expect_that(res, equals(sin(1)))

    if ("REQUEUE" %in% obj$workers_log_tail(wid, 0)$command) {
      break
    }
  }
  expect_that("REQUEUE" %in% obj$workers_log_tail(wid, 0)$command,
              is_true())

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  obj$send_signal(tools::SIGTERM, wid)
  Sys.sleep(.5)

  expect_that(pid_exists(pid), is_false())

  expect_that(obj$workers_running(wid),
              equals(setNames(TRUE, wid)))
  expect_that(obj$workers_list(), equals(wid))

  Sys.sleep(expire)

  expect_that(obj$workers_running(wid),
              equals(setNames(FALSE, wid)))
  ## Still in the workers list, though:
  expect_that(obj$workers_list(), equals(wid))

  ret <- obj$workers_identify_lost()

  expect_that(ret, equals(list(workers=wid, tasks=t$id)))
  expect_that(t$status(), equals(TASK_ORPHAN))
  expect_that(is.na(t$times()[["finished"]]), is_false())
  expect_that(obj$workers_status(wid), equals(setNames(WORKER_LOST, wid)))
  expect_that(obj$workers_list_exited(), equals(wid))

  ## We were killed in the act:
  log <- obj$workers_log_tail(wid, 1)
  expect_that(log$command, equals("TASK_START"))

  ## Then we can clean this mess up:
  res <- obj$workers_delete_exited()
  expect_that(res, equals(wid))
  expect_that(obj$workers_list_exited(), equals(character(0)))
  expect_that(obj$workers_delete_exited(), equals(character(0)))

  ## No worker keys, plenty of task keys:
  expect_that(redux::scan_find(obj$con, "tmpjobs:tasks:*"),
              not(equals(character(0))))
  expect_that(redux::scan_find(obj$con, "tmpjobs:workers:*"),
              equals(character(0)))
})

test_that("heartbeat shutdown when running job", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_that(pid_exists(pid), is_true())

  t <- obj$enqueue(slowdouble(100))
  Sys.sleep(.5)
  expect_that(obj$workers_status(wid), equals(setNames("BUSY", wid)))

  ## This message will be ignored:
  obj$send_message("STOP")
  Sys.sleep(.5)
  expect_that(obj$workers_status(wid), equals(setNames("BUSY", wid)))
  expect_that(pid_exists(pid), is_true())
  expect_that(t$status(), equals("RUNNING"))

  ## But after a message will be honoured:
  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  ## The shutdown *is* clean:
  expect_that(obj$workers_status(wid),
              equals(setNames(WORKER_EXITED, wid)))
  log <- obj$workers_log_tail(wid, 1)
  expect_that(log$command, equals("STOP"))
  expect_that(log$message, equals("OK"))
  expect_that(pid_exists(pid), is_false())

  ## The task was orphaned:
  expect_that(t$status(), equals(TASK_ORPHAN))
})

test_that("requeue orphaned jobs", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_that(pid_exists(pid), is_true())

  t_double <- 5
  t <- obj$enqueue(slowdouble(t_double))
  Sys.sleep(.5)
  expect_that(obj$workers_status(wid), equals(setNames("BUSY", wid)))
  expect_that(is.na(t$times()[["finished"]]), is_true())

  obj$send_signal(tools::SIGINT, wid)
  Sys.sleep(.5)

  expect_that(t$status(), equals(TASK_ORPHAN))
  expect_that(is.na(t$times()[["finished"]]), is_false())

  t2 <- obj$requeue(t$id)
  expect_that(t$status(), equals(TASK_REDIRECT))
  Sys.sleep(0.5)
  expect_that(t2$status(), equals(TASK_RUNNING))
  Sys.sleep(t_double)
  expect_that(t2$status(), equals(TASK_COMPLETE))

  expect_that(t$status(follow_redirect=TRUE), equals(TASK_COMPLETE))
  expect_that(t$result(follow_redirect=TRUE), equals(t_double * 2))
  expect_that(t$result(), throws_error("task [0-9]+ is unfetchable"))

  tt <- obj$tasks_times(t2$id)
  expect_that(tt, is_a("data.frame"))
  expect_that(tt$running, is_more_than(t_double - 1))
  expect_that(tt$running, is_less_than(t_double + 1))

  obj$send_message("STOP")
})


test_that("workers stop", {
  expire <- 3

  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_period=1, heartbeat_expire=expire)
  ## worker("tmpjobs", heartbeat_expire=3, heartbeat_period=1)
  ## wid <- obj$workers_list()
  pid <- obj$workers_info()[[wid]]$pid
  expect_that(pid_exists(pid), is_true())

  ## TODO: worker handles could help avoid this bit of mock up:
  expect_that(msg <- worker_stop_message(
                       list(con=obj$con,
                            keys=list(queue_name=obj$queue_name),
                            name=wid,
                            styles=worker_styles())),
              shows_message())
  cmp <- bquote(rrqueue::worker_stop(.(obj$queue_name), .(wid)))
  expect_that(parse(text=msg[[length(msg)]])[[1]],
              equals(cmp))
  eval(cmp, .GlobalEnv)

  Sys.sleep(.5)
  expect_that(pid_exists(pid), is_false())
  log <- obj$workers_log_tail(wid, 1)
  expect_that(log$command, equals("STOP"))
  expect_that(log$message, equals("OK"))
})

test_that("stop_workers (simple case)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  ok <- obj$stop_workers(wid, wait=10)
  expect_that(ok, equals(wid))

  expect_that(obj$workers_list(), equals(character(0)))
  expect_that(obj$workers_list_exited(), equals(wid))
  log <- obj$workers_log_tail(wid, 3)
  expect_that(log$command, equals(c("MESSAGE", "RESPONSE", "STOP")))
  expect_that(log$message, equals(c("STOP", "STOP", "OK")))

  res <- obj$workers_identify_lost()
  expect_that(res, equals(list(workers=character(), tasks=character())))

  expect_that(obj$workers_delete_exited(), equals(wid))
  expect_that(obj$workers_list_exited(), equals(character(0)))
  expect_that(nrow(obj$workers_log_tail(wid, 1)), equals(0))
})

test_that("stop_workers (running, interrupt)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  len <- 2
  t <- obj$enqueue(slowdouble(len))
  Sys.sleep(.25)
  expect_that(t$status(), equals(TASK_RUNNING))
  ok <- obj$stop_workers(wid, interrupt=TRUE)
  expect_that(ok, equals(wid))

  Sys.sleep(.25)

  expect_that(t$status(), equals(TASK_ORPHAN))
  expect_that(obj$workers_list(), equals(character(0)))
  expect_that(obj$workers_list_exited(), equals(wid))
  log <- obj$workers_log_tail(wid, 5)
  expect_that(log$command, equals(c("INTERRUPT", "TASK_ORPHAN",
                                    "MESSAGE", "RESPONSE", "STOP")))
  expect_that(log$message, equals(c("", t$id, "STOP", "STOP", "OK")))

  res <- obj$workers_identify_lost()
  expect_that(res, equals(list(workers=character(), tasks=character())))

  expect_that(obj$workers_delete_exited(), equals(wid))
  expect_that(obj$workers_list_exited(), equals(character(0)))
  expect_that(nrow(obj$workers_log_tail(wid, 1)), equals(0))
})

test_that("stop_workers (running, wait)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  len <- 2
  t <- obj$enqueue(slowdouble(len))
  Sys.sleep(.25)
  expect_that(t$status(), equals(TASK_RUNNING))
  ok <- obj$stop_workers(wid, interrupt=FALSE)
  expect_that(ok, equals(wid))

  Sys.sleep(len + .25)
  expect_that(t$status(), equals(TASK_COMPLETE))
  expect_that(obj$workers_list(), equals(character(0)))
  expect_that(obj$workers_list_exited(), equals(wid))
  log <- obj$workers_log_tail(wid, 1)
  expect_that(log$command, equals("STOP"))
  expect_that(log$message, equals("OK"))

  expect_that(obj$workers_delete_exited(), equals(wid))
  expect_that(obj$workers_list_exited(), equals(character(0)))
  expect_that(nrow(obj$workers_log_tail(wid, 1)), equals(0))
})

test_that("stop_workers (blocking)", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  expire <- 3
  logfile <- "worker_heartbeat.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile,
                      heartbeat_expire=expire, heartbeat_period=1)

  t <- obj$enqueue(block(10))
  Sys.sleep(.5)

  ok <- obj$stop_workers(wid, wait=1)
  expect_that(ok, equals(wid))
  expect_that(t$status(), equals(TASK_RUNNING))

  Sys.sleep(expire)
  res <- obj$workers_identify_lost()
  expect_that(res, equals(list(workers=wid, tasks=t$id)))
  expect_that(t$status(), equals(TASK_ORPHAN))
})

test_that("clean up multiple workers", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))

  expire <- 3
  n <- 4
  logfile <- sprintf("worker_%d.log", seq_len(n))
  wid <- worker_spawn(obj$queue_name, logfile, n=n,
                      heartbeat_expire=expire, heartbeat_period=1)

  expect_that(sort(obj$workers_list()), equals(sort(wid)))
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

  expect_that(sort(obj$workers_list()), equals(sort(wid)))
  x <- obj$workers_identify_lost()

  expect_that(sort(x$workers), equals(sort(wkill)))
  expect_that(sort(x$tasks), equals(sort(unname(ids[wkill]))))

  expect_that(obj$tasks_status(x$tasks),
              equals(setNames(c(TASK_ORPHAN, TASK_ORPHAN),
                              x$tasks)))
  expect_that(obj$workers_status(x$workers),
              equals(setNames(c(WORKER_LOST, WORKER_LOST), wkill)))

  obj$stop_workers()
})
