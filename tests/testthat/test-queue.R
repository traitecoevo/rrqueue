context("queue")

test_that("queue", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_that(existing, equals(character(0)))

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj, is_a("queue"))
  expect_that(obj, is_a("observer"))
  expect_that(obj$con, is_a("redis_api"))
  expect_that(obj$queue_name, equals("tmpjobs"))

  expect_that(length(obj$envirs_list()), equals(1))

  expect_that(setdiff(queues(), existing), equals("tmpjobs"))

  con <- obj$con
  keys <- rrqueue_keys(obj$queue_name)

  ## TODO: add rrqueue version here too so we know we're speaking the
  ## right dialect.

  ## There is going to be a nasty bit here because the *files* data
  ## storage creates a couple of keys that are implementation
  ## dependent.
  keys_startup <- c(keys$envirs_contents,
                    keys$envirs_files)
  tmp <- sort(as.character(con$KEYS("tmpjobs*")))
  tmp <- tmp[!grepl(paste0(keys$files, ":.*"), tmp)]
  expect_that(tmp, equals(sort(keys_startup)))

  dat <- obj$envirs_contents()[[obj$envir_id]]
  expect_that(dat$packages, equals(NULL))
  expect_that(dat$sources, equals("myfuns.R"))
  expect_that(dat$source_files, equals(hash_files("myfuns.R")))

  expect_that(obj$envirs_contents(obj$envir_id),
              equals(setNames(list(dat), obj$envir_id)))

  expect_that(obj$envirs_contents("xxx"),
              throws_error("Environment 'xxx' not found"))
  expect_that(obj$envirs_contents(c(obj$envir_id, "xxx")),
              throws_error("Environment 'xxx' not found"))
  expect_that(obj$envirs_contents(rep(obj$envir_id, 2)),
              equals(rep(setNames(list(dat), obj$envir_id), 2)))

  expect_that(obj$tasks_groups_list(), equals(character(0)))

  ## Queue two tasks:
  grp <- "mygroup"
  task1 <- obj$enqueue(sin(1))
  task2 <- obj$enqueue(sin(2), group=grp)

  expect_that(task1, is_a("task"))

  expect_that(task1$id, equals("1"))
  expect_that(task2$id, equals("2"))
  expect_that(con$GET(keys$tasks_counter), equals("2"))

  expect_that(obj$tasks_list(), equals(c("1", "2")))
  expect_that(obj$tasks_status(),
              equals(c("1"=TASK_PENDING, "2"=TASK_PENDING)))
  expect_that(task1$expr(), equals(quote(sin(1))))

  ## as.list because 'table' objects are weird.
  expect_that(as.list(obj$tasks_overview()),
              equals(list(PENDING=2, RUNNING=0, COMPLETE=0, ERROR=0)))

  expect_that(obj$tasks_envir(),
              equals(c("1"=obj$envir_id, "2"=obj$envir_id)))

  t <- obj$tasks_times()
  expect_that(t, is_a("data.frame"))
  now <- redis_time_to_r(RedisAPI::redis_time(obj$con))
  expect_that(all(t$submitted <= now), is_true())
  expect_that(all(t$waiting >= 0.0), is_true())
  expect_that(all(is.na(t$started)), is_true())
  expect_that(t$started, is_a("POSIXct"))
  expect_that(names(t), equals(c("submitted", "started", "finished",
                                 "waiting", "running", "idle")))

  key_queue <- rrqueue_key_queue(obj$queue_name, obj$envir_id)
  keys_tasks <- c(keys$tasks_expr, keys$tasks_counter,
                  keys$tasks_status, keys$tasks_envir,
                  keys$tasks_complete, keys$tasks_group,
                  keys$tasks_time_sub, key_queue)

  tmp <- sort(as.character(RedisAPI::scan_find(con, "tmpjobs*")))
  tmp <- tmp[!grepl(paste0(keys$files, ":.*"), tmp)]
  expect_that(tmp,
              equals(sort(c(keys_startup, keys_tasks))))

  expect_that(con$TYPE(key_queue),           equals("list"))
  expect_that(con$TYPE(keys$tasks_expr),     equals("hash"))
  expect_that(con$TYPE(keys$tasks_counter),  equals("string"))
  expect_that(con$TYPE(keys$tasks_status),   equals("hash"))
  expect_that(con$TYPE(keys$tasks_complete), equals("hash"))
  expect_that(con$TYPE(keys$tasks_group),    equals("hash"))
  expect_that(con$TYPE(keys$tasks_envir),    equals("hash"))
  expect_that(con$TYPE(keys$tasks_time_sub), equals("hash"))

  ids <- con$LRANGE(key_queue, 0, -1)
  expect_that(ids, equals(list("1", "2")))
  ids <- as.character(ids) # unlist

  expect_that(task1$expr(), equals(quote(sin(1))))

  expect_that(task2$expr(), equals(quote(sin(2))))
  tmp <- task2$expr(locals=TRUE)
  expect_that(attr(tmp, "envir"), is_a("environment"))
  expect_that(ls(attr(tmp, "envir")), equals(character(0)))
  attr(tmp, "envir") <- NULL
  expect_that(tmp, equals(quote(sin(2))))

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

  expect_that(obj$con$HGET(keys$tasks_group, ids[[1]]), is_null())
  expect_that(obj$con$HGET(keys$tasks_group, ids[[2]]), equals(grp))

  expect_that(obj$tasks_groups_list(), equals(grp))
  expect_that(obj$tasks_in_groups(grp), equals(ids[[2]]))
  expect_that(obj$tasks_in_groups("xxx"), equals(character(0)))

  grp2 <- create_group(NULL, FALSE)
  grp3 <- create_group(NULL, FALSE)
  obj$tasks_set_group(ids[[1]], grp2)
  expect_that(sort(obj$tasks_groups_list()), equals(sort(c(grp, grp2))))
  expect_that(obj$tasks_in_groups(grp2), equals(ids[[1]]))

  ## No error when setting a group to the same thing:
  expect_that(obj$tasks_set_group(ids[[1]], grp2),
              not(throws_error()))
  msg <- paste("Groups already exist for tasks:", ids[[1]])
  expect_that(obj$tasks_set_group(ids[[1]], grp3),
              throws_error(msg))
  expect_that(obj$tasks_set_group(ids[[1]], grp3, "warn"),
              gives_warning(msg))
  expect_that(obj$tasks_set_group(ids[[1]], grp3, "pass"),
              not(gives_warning()))
  expect_that(obj$tasks_in_groups(grp2), equals(ids[[1]]))
  expect_that(obj$tasks_set_group(ids[[1]], grp3, "overwrite"),
              not(throws_error()))
  expect_that(obj$tasks_in_groups(grp2), equals(character(0)))
  expect_that(obj$tasks_in_groups(grp3), equals(ids[[1]]))

  ## Delete the groups:
  obj$tasks_set_group(ids, NULL)
  expect_that(sort(obj$tasks_groups_list()), equals(character(0)))

  expect_that(task1$status(), equals(TASK_PENDING))
  expect_that(task2$status(), equals(TASK_PENDING))
  expect_that(task1$result(), throws_error("unfetchable: PENDING"))
  expect_that(task2$result(), throws_error("unfetchable: PENDING"))
  expect_that(task1$key_complete,
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[1]])))
  expect_that(task2$key_complete,
              equals(rrqueue_key_task_complete(obj$queue_name, ids[[2]])))

  expect_that(obj$tasks_drop(ids), equals(setNames(c(TRUE, TRUE), ids)))
  expect_that(obj$tasks_drop(ids), equals(setNames(c(FALSE, FALSE), ids)))
  ## TODO:
  ## expect_that(obj$tasks(), equals(empty_named_character()))
  expect_that(con$LRANGE(key_queue, 0, -1), equals(list()))

  expect_that(task1$status(), equals(TASK_MISSING))
  expect_that(task2$status(), equals(TASK_MISSING))

  ## This needs to send output to a file and not to stdout!
  logfile <- "worker.log"
  ## See: https://github.com/hadley/testthat/issues/144
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)
  ## or!
  ##   w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  expect_that(obj$workers_len(), equals(1))

  expect_that(con$TYPE(keys$workers_status), equals("hash"))
  expect_that(con$TYPE(keys$workers_name),   equals("set"))

  w <- obj$workers_list()
  expect_that(length(w), equals(1L))
  expect_that(w, is_identical_to(wid))

  ww <- parse_worker_name(w)
  expect_that(ww$host, equals(Sys.info()[["nodename"]]))

  expect_that(obj$workers_status(),
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
  ## expect_that(restore_expression(obj$tasks()[["3"]], e, obj$objects),
  ##             equals(quote(sin(1))))
  ## expect_that(ls(e), equals(character(0)))

  expect_that(obj$tasks_status()[[task$id]], equals(TASK_COMPLETE))
  expect_that(obj$task_result(task$id),     equals(sin(1)))

  expect_that(task$status(), equals(TASK_COMPLETE))
  expect_that(task$result(), equals(sin(1)))

  ## Another, using arguments:
  x <- 1
  e <- environment()
  task <- obj$enqueue(sin(x), e)

  done <- obj$con$BLPOP(task$key_complete, 10)
  expect_that(obj$tasks_status()[[task$id]], equals(TASK_COMPLETE))
  expect_that(obj$task_result(task$id), equals(sin(x)))

  ## TODO: factor out the mangling here.
  expect_that(obj$objects$list(),
              equals(paste0(task_object_prefix(task$id), "x")))
  ## TODO:
  ## e2 <- new.env(parent=.GlobalEnv)
  ## expect_that(restore_expression(obj$tasks()[[task$id]], e2, obj$objects),
  ##             equals(quote(sin(x))))
  ## expect_that(ls(e2), equals("x"))

  ## TODO:
  ## on job removal, clean up objects that were created.  That's easy
  ## enough to set up with enqueue, and then trigger later.

  obj$send_message("STOP", w)
  Sys.sleep(.5)
  expect_that(obj$workers_len(), equals(0))
  expect_that(obj$workers_list(), equals(character(0)))

  expect_that(obj$workers_list_exited(), equals(wid))
  expect_that(obj$workers_log_tail(wid)[["command"]], equals("STOP"))
  expect_that(obj$workers_status(wid), equals(setNames(NA_character_, wid)))

  dlog <- obj$workers_log_tail(w, n=0)
  expect_that(dlog, is_a("data.frame"))

  expect_that(dlog$command, equals(c("ALIVE", "ENVIR",
                                     "TASK_START", "TASK_COMPLETE",
                                     "TASK_START", "TASK_COMPLETE",
                                     "MESSAGE", "RESPONSE", "STOP")))
  expect_that(dlog$message, equals(c("", obj$envir_id, "3", "3", "4", "4",
                                     "STOP", "STOP", "OK")))

  ## TODO: cleanup properly.
  test_cleanup()
})

test_that("worker responses", {
  test_cleanup()
  on.exit(test_cleanup())
  obj <- queue("tmpjobs", sources="myfuns.R")
  con <- obj$con

  wid <- worker_spawn(obj$queue_name, "worker.log")
  ## or!
  ##   w <- rrqueue::worker("tmpjobs")

  wid <- obj$workers_list()
  response <- rrqueue_key_worker_response(obj$queue_name, wid)
  expect_that(con$HLEN(response), equals(0L))

  ## First, PING:
  id <- obj$send_message("PING")
  ## sending messages returns the set of workers that the message was
  ## sent to.  That makes it easier to retrieve messages from their
  ## queue.
  Sys.sleep(.2)

  expect_that(obj$has_responses(id),
              equals(setNames(TRUE, wid)))
  expect_that(obj$response_ids(wid), equals(id))

  res <- obj$get_responses(id)
  expect_that(res, equals(setNames(list("PONG"), wid)))
  res <- obj$get_responses(id, delete=TRUE)
  expect_that(res, equals(setNames(list("PONG"), wid)))
  expect_that(obj$get_responses(id, delete=TRUE),
              throws_error("Response missing for workers"))

  expect_that(obj$has_responses(id), equals(setNames(FALSE, wid)))
  expect_that(obj$response_ids(wid), equals(character(0)))

  id <- obj$send_message("ECHO", "hello")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_that(res, equals("OK"))
  expect_that(obj$get_response(id, wid, delete=TRUE),
              throws_error("Response missing for workers"))

  id <- obj$send_message("EVAL", "1 + 1")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_that(res, equals(2))

  id <- obj$send_message("INFO")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_that(res, is_a("worker_info"))
  expect_that(res$worker, equals(wid))

  ## I think I'm not getting the ids correct here.
  id <- obj$send_message("STOP")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_that(res, equals("BYE"))
})

test_that("cleanup", {
  test_cleanup()
  queues()
  obj <- queue("tmpjobs", sources="myfuns.R")
  obj$enqueue(Sys.sleep(20))
  wid <- worker_spawn(obj$queue_name, "worker.log")
  ## or!
  ##   w <- rrqueue::worker("tmpjobs")

  info <- obj$workers_info()
  pid <- info[[1]]$pid
  expect_that(pid_exists(pid), is_true())

  ## Better; poll for a change in the status...
  Sys.sleep(.5)
  expect_that(obj$workers_status(), equals(setNames("BUSY", wid)))

  queue_clean(obj$con, "tmpjobs",
              stop_workers=TRUE, kill_local=TRUE, wait_stop=0.1)
  Sys.sleep(.5)

  expect_that(pid_exists(pid), is_false())
  test_cleanup()
})

test_that("worker-first startup", {
  test_cleanup()

  wid <- worker_spawn("tmpjobs", "worker.log")
  ## or!
  ##   w <- rrqueue::worker("tmpjobs")

  obs <- observer("tmpjobs")
  expect_that(obs$workers_list(), equals(wid))
  ## No environments:
  expect_that(obs$workers_info()[[wid]]$envir, equals(character(0)))
  expect_that(obs$worker_envir(wid), equals(character(0)))

  ## startup a queue:
  obj <- queue("tmpjobs", sources="myfuns.R")
  envir_id <- obj$envir_id

  ## This is not updated:
  expect_that(obs$workers_info()[[1]]$envir, equals(character(0)))
  ## But this is (might need a little sleep here)
  Sys.sleep(.1)
  expect_that(obs$worker_envir(wid), equals(envir_id))

  ## Ask the worker to update the environment:
  obj$send_message("INFO")
  Sys.sleep(.1) # Do a BLPOP on the response queue.
  expect_that(obs$workers_info()[[1]]$envir, equals(envir_id))

  expect_that(obj$envirs_list(), equals(envir_id))
  expect_that(obj$envir_workers(envir_id),
              equals(setNames(TRUE, wid)))

  t <- obj$enqueue(sin(1))
  Sys.sleep(.1)
  expect_that(t$result(), equals(sin(1)))

  obj$send_message("STOP")
  Sys.sleep(.1)

  expect_that(obj$envir_workers(envir_id),
              equals(setNames(TRUE, wid)[-1]))
})

test_that("file info", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_that(existing, equals(character(0)))

  filename <- "myfuns.R"
  obj <- queue("tmpjobs", sources=filename)
  dat <- string_to_object(obj$con$HGET(obj$keys$envirs_files, obj$envir_id))

  tmp <- tempfile("rrqueue_")
  files_unpack(obj$files, dat, tmp)
  expect_that(dir(tmp), equals(filename))
  expect_that(hash_file(file.path(tmp, filename)),
              equals(hash_file(filename)))
})

test_that("fetch files", {
  test_cleanup()
  queues()
  obj <- queue("tmpjobs", sources="myfuns.R")

  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, "worker.log")
  ## or!
  ##   w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  id <- obj$send_message("PUSH", "myfuns.R")
  dat <- obj$get_response(id, wid, wait=5, delete=TRUE)

  tmp <- tempfile("rrqueue_")
  files_unpack(obj$files, dat, tmp)
  expect_that(dir(tmp), equals("myfuns.R"))
  expect_that(hash_file(file.path(tmp, "myfuns.R")),
              equals(hash_file("myfuns.R")))

  obj$send_message("STOP")
})

test_that("empty start", {
  path <- tempfile("rrqueue_")
  dir.create(path)

  test_cleanup()
  queues()
  obj <- queue("tmpjobs", sources="myfuns.R")

  Sys.setenv("R_TESTS" = "")
  logfile <- tempfile("rrqueue_", fileext=".log")
  wid <- worker_spawn(obj$queue_name, logfile, path=path)

  expect_that(file.exists(logfile), is_true())

  ## The directory is empty:
  id <- obj$send_message("DIR")
  dat <- obj$get_response(id, wid, wait=5, delete=TRUE)
  expect_that(dat, equals(empty_named_character()))

  ## But we still do have a worker listening on this queue:
  expect_that(obj$worker_envir(wid), equals(obj$envir_id))
  expect_that(obj$envir_workers(obj$envir_id), equals(setNames(TRUE, wid)))

  ## We can pull files onto the worker too:
  res <- obj$files_pack("myfuns.R")
  dat <- obj$envirs_contents(obj$envir_id)[[1]]$source_files
  id <- obj$send_message("PULL", dat)
  expect_that(obj$get_response(id, wid, wait=5, delete=TRUE), equals("OK"))

  id <- obj$send_message("DIR")
  dat <- obj$get_response(id, wid, wait=5)
  expect_that(dat, equals(hash_files("myfuns.R")))

  ## can also exectute commands on the worker:
  id <- obj$send_message("EVAL", "getwd()", wid)
  dat <- obj$get_response(id, wid, wait=5)
  ## Might be platform dependent, unfortunately.
  expect_that(dat, equals(normalizePath(path)))

  id <- obj$send_message("STOP")
  dat <- obj$get_response(id, wid, wait=5)
  expect_that(dat, equals("BYE"))
})

test_that("complex expressions", {
  obj <- queue("tmpjobs")
  expect_that(obj$enqueue(sin(cos(1))),
              throws_error("complex expressions not yet supported"))
})

test_that("controlled failures", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  wid <- worker_spawn(obj$queue_name, tempfile())

  t <- obj$enqueue(failure(controlled=TRUE))
  Sys.sleep(.1)
  expect_that(t$status(), equals("COMPLETE"))
  expect_that(is_error(t$result()), is_true())
  expect_that(t$result(), not(is_a("WorkerTaskError")))

  t <- obj$enqueue(failure(controlled=FALSE))
  Sys.sleep(.1)
  expect_that(t$status(), equals("ERROR"))
  expect_that(is_error(t$result()), is_true())
  expect_that(t$result(), is_a("WorkerTaskError"))

  obj$send_message("STOP")
})

test_that("wait", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  t <- obj$enqueue(slowdouble(0.3))
  expect_that(t$wait(0), throws_error("task not returned in time"))
  expect_that(t$wait(0.1), throws_error("task not returned in time"))

  wid <- worker_spawn(obj$queue_name, tempfile())
  expect_that(t$wait(1), equals(0.6))
  expect_that(t$wait(1), takes_less_than(1))

  t <- obj$enqueue(slowdouble(1))
  expect_that(t$wait(0), throws_error("task not returned in time"))
  expect_that(t$wait(2), equals(2))
  obj$send_message("STOP")
  Sys.sleep(.5)
})

test_that("stop workers", {
  test_cleanup()

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$workers_list_exited(), equals(character(0)))
  wid <- worker_spawn(obj$queue_name, tempfile())
  expect_that(obj$workers_list(), equals(wid))
  pid <- obj$workers_info()[[wid]]$pid
  expect_that(pid_exists(pid), is_true())

  ## Queue up a job that will outlive the test:
  obj$enqueue(slowdouble(30))

  Sys.sleep(.5)
  expect_that(obj$workers_status(), equals(setNames("BUSY", wid)))

  ## This is going to give nothing sensible here:
  obj$stop_workers(wid)
  Sys.sleep(.5)
  expect_that(obj$workers_status(), equals(setNames("BUSY", wid)))
  expect_that(pid_exists(pid), is_true())

  obj$stop_workers(wid, kill_local=TRUE)
  Sys.sleep(.2)
  expect_that(pid_exists(pid), is_false())
  expect_that(obj$workers_list(), equals(character(0)))
  expect_that(obj$workers_list_exited(), equals(wid))
})

test_that("config", {
  obj <- queue(sources="myfuns.R", config="config.yml")
  dat <- load_config("config.yml")
  expect_that(obj$queue_name, equals(dat$queue_name))
  expect_that(obj$con$host, equals(dat$redis_host))
  expect_that(obj$con$port, equals(dat$redis_port))

  env <- obj$envirs_contents()[[obj$envir_id]]
  expect_that(env$packages, equals(dat$packages))
  expect_that(env$sources, equals(dat$sources))
})

test_that("config observer", {
  obj <- observer(config="config.yml")
  dat <- load_config("config.yml")
  expect_that(obj$queue_name, equals(dat$queue_name))
  expect_that(obj$con$host, equals(dat$redis_host))
  expect_that(obj$con$port, equals(dat$redis_port))
})
