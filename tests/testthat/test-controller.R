context("controller")

test_that("controller", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_that(existing, equals(character(0)))

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$con, is_a("redis_api"))
  expect_that(obj$queue_name, equals("tmpjobs"))

  expect_that(setdiff(queues(), existing), equals("tmpjobs"))

  keys <- rrqueue_keys(obj$queue_name)

  con <- obj$con

  ## TODO: add controller version here too so we know we're speaking
  ## the right dialect.
  keys_startup <- c(keys$envirs_contents)
  expect_that(sort(as.character(con$KEYS("tmpjobs*"))),
              equals(sort(keys_startup)))

  dat <- string_to_object(con$HGET(keys$envirs_contents, obj$envir_id))
  expect_that(dat$packages, equals(NULL))
  expect_that(dat$sources, equals("myfuns.R"))
  expect_that(dat$source_files, equals(hash_file("myfuns.R")))

  ## Queue two tasks:
  task1 <- obj$enqueue(sin(1))
  task2 <- obj$enqueue(sin(2))

  expect_that(task1, is_a("task"))

  expect_that(task1$id, equals("1"))
  expect_that(task2$id, equals("2"))

  ## TODO: This might change.
  expect_that(obj$tasks_status(),
              equals(c("1"=TASK_PENDING, "2"=TASK_PENDING)))

  keys_tasks <- c(keys$tasks_expr, keys$tasks_counter, keys$tasks_id,
                  keys$tasks_status, keys$tasks_envir,
                  keys$tasks_complete, keys$tasks_time_sub)
  expect_that(sort(as.character(con$KEYS("tmpjobs*"))),
              equals(sort(c(keys_startup, keys_tasks))))

  expect_that(con$TYPE(keys$tasks_id),       equals("list"))
  expect_that(con$TYPE(keys$tasks_expr),     equals("hash"))
  expect_that(con$TYPE(keys$tasks_counter),  equals("string"))
  expect_that(con$TYPE(keys$tasks_status),   equals("hash"))
  expect_that(con$TYPE(keys$tasks_complete), equals("hash"))
  expect_that(con$TYPE(keys$tasks_envir),    equals("hash"))
  expect_that(con$TYPE(keys$tasks_time_sub), equals("hash"))

  ids <- con$LRANGE(keys$tasks_id, 0, -1)
  expect_that(ids, equals(list("1", "2")))
  ids <- as.character(ids) # unlist

  ## TODO: simplify this:
  tasks_expr <- unname(from_redis_hash(con, keys$tasks_expr)[ids])
  e <- new.env(parent=.GlobalEnv)
  tasks <- lapply(tasks_expr, restore_expression, e, obj$objects)
  expect_that(ls(e), equals(character(0)))
  expect_that(tasks, equals(list(quote(sin(1)), quote(sin(2)))))

  ## TODO: check that envir is 1 and that the complete queue is empty,
  ## but that it is registered

  expect_that(obj$con$HGET(keys$tasks_envir, ids[[1]]),
              equals(obj$envir_id))
  expect_that(obj$con$HGET(keys$tasks_envir, ids[[2]]),
              equals(obj$envir_id))
  expect_that(obj$con$HGET(keys$tasks_complete, ids[[1]]),
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[1]])))
  expect_that(obj$con$HGET(keys$tasks_complete, ids[[2]]),
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[2]])))

  expect_that(con$GET(keys$tasks_counter), equals("2"))

  status <- from_redis_hash(con, keys$tasks_status)
  expect_that(status[ids],
              equals(setNames(rep(TASK_PENDING, length(ids)), ids)))

  expect_that(task1$status(), equals(TASK_PENDING))
  expect_that(task2$status(), equals(TASK_PENDING))
  expect_that(task1$result(), throws_error("incomplete"))
  expect_that(task2$result(), throws_error("incomplete"))
  expect_that(task1$key_complete,
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[1]])))
  expect_that(task2$key_complete,
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[2]])))

  expect_that(obj$tasks(), equals(from_redis_hash(con, keys$tasks_expr)))

  expect_that(obj$tasks_drop(ids), equals(setNames(c(TRUE, TRUE), ids)))
  expect_that(obj$tasks_drop(ids), equals(setNames(c(FALSE, FALSE), ids)))
  expect_that(obj$tasks(), equals(empty_named_character()))
  expect_that(con$LRANGE(keys$tasks_id, 0, -1), equals(list()))

  expect_that(task1$status(), equals(TASK_MISSING))
  expect_that(task2$status(), equals(TASK_MISSING))

  ## This needs to send output to a file and not to stdout!
  logfile <- "worker.log"
  ## See: https://github.com/hadley/testthat/issues/144
  Sys.setenv("R_TESTS" = "")
  wid <- rrqueue_worker_spawn(obj$queue_name, logfile)
  ## or!
  ##   w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  expect_that(obj$n_workers(), equals(1))

  expect_that(con$TYPE(keys$workers_status), equals("hash"))
  expect_that(con$TYPE(keys$workers_name),   equals("set"))

  w <- obj$workers()
  expect_that(length(w), equals(1L))
  expect_that(w, is_identical_to(wid))

  ww <- parse_worker_name(w)
  expect_that(ww$host, equals(Sys.info()[["nodename"]]))

  expect_that(from_redis_hash(con, keys$workers_status),
              equals(structure(as.character(WORKER_IDLE),
                               names=w[[1]])))

  ## OK, submit a trivial job
  task <- obj$enqueue(sin(1))

  ## Blocking check for job completion.
  done <- obj$con$BLPOP(task$key_complete, 10)
  expect_that(done[[1]], equals(task$key_complete))
  expect_that(done[[2]], equals(task$id))

  ## TODO: if the test above fails, everything below here will pack a sad.

  ## TODO: Fix $tasks() here
  expect_that(restore_expression(obj$tasks()[["3"]], e, obj$objects),
              equals(quote(sin(1))))
  expect_that(ls(e), equals(character(0)))

  expect_that(obj$tasks_status()[[task$id]], equals(TASK_COMPLETE))
  expect_that(obj$tasks_collect(task$id),    equals(sin(1)))

  expect_that(task$status(), equals(TASK_COMPLETE))
  expect_that(task$result(), equals(sin(1)))

  ## Another, using arguments:
  x <- 1
  e <- environment()
  task <- obj$enqueue(sin(x), e)

  done <- obj$con$BLPOP(task$key_complete, 10)
  expect_that(obj$tasks_status()[[task$id]], equals(TASK_COMPLETE))
  expect_that(obj$tasks_collect(task$id), equals(sin(x)))

  ## TODO: factor out the mangling here.
  expect_that(obj$objects$list(),
              equals(paste0(task_object_prefix(task$id), "x")))
  e2 <- new.env(parent=.GlobalEnv)
  expect_that(restore_expression(obj$tasks()[[task$id]], e2, obj$objects),
              equals(quote(sin(x))))
  expect_that(ls(e2), equals("x"))

  ## TODO:
  ## on job removal, clean up objects that were created.  That's easy
  ## enough to set up with enqueue, and then trigger later.

  ## TODO: get statistics off the workers about completed jobs
  ## perhaps?  Can be done by parsing the worker log.

  obj$send_message("STOP", w)
  Sys.sleep(.5)
  expect_that(obj$n_workers(), equals(0))
  expect_that(obj$workers(), equals(character(0)))

  log_key <- rrqueue_key_worker_log(obj$queue_name, w)
  log <- as.character(obj$con$LRANGE(log_key, 0, -1))
  dlog <- parse_worker_log(log)
  expect_that(dlog, is_a("data.frame"))

  expect_that(dlog$command, equals(c("ALIVE",
                                     "TASK_START", "ENVIR", "TASK_COMPLETE",
                                     "TASK_START", "TASK_COMPLETE",
                                     "MESSAGE", "STOP")))
  expect_that(dlog$message, equals(c("", "3", obj$envir_id, "3", "4", "4",
                                     "STOP", "OK")))

  ## TODO: cleanup properly.
})
