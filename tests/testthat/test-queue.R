context("queue")

test_that("queue", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_equal(existing, character(0))

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_is(obj, "queue")
  expect_is(obj, "observer")
  expect_is(obj$con, "redis_api")
  expect_equal(obj$queue_name, "tmpjobs")

  expect_equal(length(obj$envirs_list()), 1)

  expect_equal(setdiff(queues(), existing), "tmpjobs")

  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$workers_list(TRUE), character(0))

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
  expect_equal(tmp, sort(keys_startup))

  dat <- obj$envirs_contents()[[obj$envir_id]]
  expect_null(dat$packages)
  expect_equal(dat$sources, "myfuns.R")
  expect_equal(dat$source_files, hash_files("myfuns.R"))

  expect_equal(obj$envirs_contents(obj$envir_id),
               setNames(list(dat), obj$envir_id))

  expect_error(obj$envirs_contents("xxx"),
               "Environment 'xxx' not found")
  expect_error(obj$envirs_contents(c(obj$envir_id, "xxx")),
               "Environment 'xxx' not found")
  expect_equal(obj$envirs_contents(rep(obj$envir_id, 2)),
               rep(setNames(list(dat), obj$envir_id), 2))

  expect_equal(obj$tasks_groups_list(), character(0))

  ## Queue two tasks:
  grp <- "mygroup"
  task1 <- obj$enqueue(sin(1))
  task2 <- obj$enqueue(sin(2), group=grp)

  expect_is(task1, "task")

  expect_equal(task1$id, "1")
  expect_equal(task2$id, "2")
  expect_equal(con$GET(keys$tasks_counter), "2")

  expect_equal(obj$tasks_list(), c("1", "2"))
  expect_equal(obj$tasks_status(),
               c("1"=TASK_PENDING, "2"=TASK_PENDING))
  expect_equal(task1$expr(), quote(sin(1)))

  ## as.list because 'table' objects are weird.
  expect_equal(as.list(obj$tasks_overview()),
               list(PENDING=2, RUNNING=0, COMPLETE=0, ERROR=0))

  expect_equal(obj$tasks_envir(),
               c("1"=obj$envir_id, "2"=obj$envir_id))

  t <- obj$tasks_times()
  expect_is(t, "data.frame")
  now <- redux::redis_time_to_r(redux::redis_time(obj$con))
  expect_equal(t$task_id, obj$tasks_list())
  expect_true(all(t$submitted <= now))
  expect_true(all(t$waiting >= 0.0))
  expect_true(all(is.na(t$started)))
  expect_is(t$started, "POSIXct")
  expect_equal(names(t), c("task_id",
                           "submitted", "started", "finished",
                           "waiting", "running", "idle"))

  key_queue <- rrqueue_key_queue(obj$queue_name, obj$envir_id)
  keys_tasks <- c(keys$tasks_expr, keys$tasks_counter,
                  keys$tasks_status, keys$tasks_envir,
                  keys$tasks_complete, keys$tasks_group,
                  keys$tasks_time_sub, key_queue)

  tmp <- sort(as.character(redux::scan_find(con, "tmpjobs*")))
  tmp <- tmp[!grepl(paste0(keys$files, ":.*"), tmp)]
  expect_equal(tmp,
               sort(c(keys_startup, keys_tasks)))

  redis_status <- function(x) structure(x, class="redis_status")
  expect_equal(con$TYPE(key_queue),           redis_status("list"))
  expect_equal(con$TYPE(keys$tasks_expr),     redis_status("hash"))
  expect_equal(con$TYPE(keys$tasks_counter),  redis_status("string"))
  expect_equal(con$TYPE(keys$tasks_status),   redis_status("hash"))
  expect_equal(con$TYPE(keys$tasks_complete), redis_status("hash"))
  expect_equal(con$TYPE(keys$tasks_group),    redis_status("hash"))
  expect_equal(con$TYPE(keys$tasks_envir),    redis_status("hash"))
  expect_equal(con$TYPE(keys$tasks_time_sub), redis_status("hash"))

  ids <- con$LRANGE(key_queue, 0, -1)
  expect_equal(ids, list("1", "2"))
  ids <- as.character(ids) # unlist

  expect_equal(task1$expr(), quote(sin(1)))

  expect_equal(task2$expr(), quote(sin(2)))
  tmp <- task2$expr(locals=TRUE)
  expect_is(attr(tmp, "envir"), "environment")
  expect_equal(ls(attr(tmp, "envir")), character(0))
  attr(tmp, "envir") <- NULL
  expect_equal(tmp, quote(sin(2)))

  ## TODO: check that envir is 1 and that the complete queue is empty,
  ## but that it is registered

  expect_equal(obj$con$HGET(keys$tasks_envir, ids[[1]]),
               obj$envir_id)
  expect_equal(obj$con$HGET(keys$tasks_envir, ids[[2]]),
               obj$envir_id)
  expect_equal(obj$con$HGET(keys$tasks_complete, ids[[1]]),
               rrqueue_key_task_complete(obj$queue_name, ids[[1]]))
  expect_equal(obj$con$HGET(keys$tasks_complete, ids[[2]]),
               rrqueue_key_task_complete(obj$queue_name, ids[[2]]))

  expect_null(obj$con$HGET(keys$tasks_group, ids[[1]]))
  expect_equal(obj$con$HGET(keys$tasks_group, ids[[2]]), grp)

  expect_equal(obj$tasks_groups_list(), grp)
  expect_equal(obj$tasks_in_groups(grp), ids[[2]])
  expect_equal(obj$tasks_in_groups("xxx"), character(0))

  grp2 <- create_group(NULL, FALSE)
  grp3 <- create_group(NULL, FALSE)
  obj$tasks_set_group(ids[[1]], grp2)
  expect_equal(sort(obj$tasks_groups_list()), sort(c(grp, grp2)))
  expect_equal(obj$tasks_in_groups(grp2), ids[[1]])

  ## No error when setting a group to the same thing:
  expect_silent(obj$tasks_set_group(ids[[1]], grp2))
  msg <- paste("Groups already exist for tasks:", ids[[1]])
  expect_error(obj$tasks_set_group(ids[[1]], grp3), msg)
  expect_warning(obj$tasks_set_group(ids[[1]], grp3, "warn"), msg)
  expect_silent(obj$tasks_set_group(ids[[1]], grp3, "pass"))
  expect_equal(obj$tasks_in_groups(grp2), ids[[1]])
  expect_silent(obj$tasks_set_group(ids[[1]], grp3, "overwrite"))
  expect_equal(obj$tasks_in_groups(grp2), character(0))
  expect_equal(obj$tasks_in_groups(grp3), ids[[1]])

  ## Delete the groups:
  obj$tasks_set_group(ids, NULL)
  expect_equal(sort(obj$tasks_groups_list()), character(0))

  expect_equal(task1$status(), TASK_PENDING)
  expect_equal(task2$status(), TASK_PENDING)
  expect_error(task1$result(), "unfetchable: PENDING")
  expect_error(task2$result(), "unfetchable: PENDING")
  expect_equal(task1$key_complete,
               rrqueue_key_task_complete(obj$queue_name, ids[[1]]))
  expect_equal(task2$key_complete,
               rrqueue_key_task_complete(obj$queue_name, ids[[2]]))

  expect_equal(obj$tasks_drop(ids), setNames(c(TRUE, TRUE), ids))
  expect_equal(obj$tasks_drop(ids), setNames(c(FALSE, FALSE), ids))
  ## TODO:
  ## expect_equal(obj$tasks(), empty_named_character()))
  expect_equal(con$LRANGE(key_queue, 0, -1), list())

  expect_equal(task1$status(), TASK_MISSING)
  expect_equal(task2$status(), TASK_MISSING)

  ## This needs to send output to a file and not to stdout!
  logfile <- "worker.log"
  ## See: https://github.com/hadley/testthat/issues/144
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)
  ## or!
  ##   w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  expect_equal(obj$workers_len(), 1)

  expect_equal(con$TYPE(keys$workers_status), redis_status("hash"))
  expect_equal(con$TYPE(keys$workers_name),   redis_status("set"))

  w <- obj$workers_list()
  expect_equal(length(w), 1L)
  expect_identical(w, wid)
  expect_equal(obj$workers_list(envir_only=TRUE), wid)

  ww <- parse_worker_name(w)
  expect_equal(ww$host, Sys.info()[["nodename"]])

  expect_equal(obj$workers_status(),
               structure(as.character(WORKER_IDLE),
                         names=w[[1]]))

  ## OK, submit a trivial job
  task <- obj$enqueue(sin(1))

  ## Blocking check for job completion.
  done <- obj$con$BLPOP(task$key_complete, 10)
  expect_equal(done[[1]], task$key_complete)
  expect_equal(done[[2]], task$id)

  ## TODO: if the test above fails, everything below here will pack a sad.

  ## TODO: Fix $tasks() here
  ## expect_equal(restore_expression(obj$tasks()[["3"]], e, obj$objects),
  ##             quote(sin(1))))
  ## expect_equal(ls(e), character(0))

  expect_equal(obj$tasks_status()[[task$id]], TASK_COMPLETE)
  expect_equal(obj$task_result(task$id),     sin(1))

  expect_equal(task$status(), TASK_COMPLETE)
  expect_equal(task$result(), sin(1))

  ## Another, using arguments:
  x <- 1
  e <- environment()
  task <- obj$enqueue(sin(x), e)

  done <- obj$con$BLPOP(task$key_complete, 10)
  expect_equal(obj$tasks_status()[[task$id]], TASK_COMPLETE)
  expect_equal(obj$task_result(task$id), sin(x))

  ## TODO: factor out the mangling here.
  expect_equal(obj$objects$list(),
               paste0(task_object_prefix(task$id), "x"))
  ## TODO:
  ## e2 <- new.env(parent=.GlobalEnv)
  ## expect_equal(restore_expression(obj$tasks()[[task$id]], e2, obj$objects),
  ##             quote(sin(x)))
  ## expect_equal(ls(e2), "x")

  ## TODO:
  ## on job removal, clean up objects that were created.  That's easy
  ## enough to set up with enqueue, and then trigger later.

  obj$send_message("STOP", w)
  Sys.sleep(.5)
  expect_equal(obj$workers_len(), 0)
  expect_equal(obj$workers_list(), character(0))

  expect_equal(obj$workers_list_exited(), wid)
  expect_equal(obj$workers_list_exited(TRUE), wid)
  expect_equal(obj$workers_log_tail(wid)[["command"]], "STOP")
  expect_equal(obj$workers_status(wid), setNames("EXITED", wid))

  dlog <- obj$workers_log_tail(w, n=0)
  expect_is(dlog, "data.frame")

  expect_equal(dlog$command, c("ALIVE", "ENVIR",
                               "TASK_START", "TASK_COMPLETE",
                               "TASK_START", "TASK_COMPLETE",
                               "MESSAGE", "RESPONSE", "STOP"))
  expect_equal(dlog$message, c("", obj$envir_id, "3", "3", "4", "4",
                               "STOP", "STOP", "OK"))

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
  expect_equal(con$HLEN(response), 0L)

  ## First, PING:
  id <- obj$send_message("PING")
  ## sending messages returns the set of workers that the message was
  ## sent to.  That makes it easier to retrieve messages from their
  ## queue.
  Sys.sleep(.2)

  expect_equal(obj$has_responses(id),
               setNames(TRUE, wid))
  expect_equal(obj$response_ids(wid), id)

  res <- obj$get_responses(id)
  expect_equal(res, setNames(list("PONG"), wid))
  res <- obj$get_responses(id, delete=TRUE)
  expect_equal(res, setNames(list("PONG"), wid))
  expect_error(obj$get_responses(id, delete=TRUE),
               "Response missing for workers")

  expect_equal(obj$has_responses(id), setNames(FALSE, wid))
  expect_equal(obj$response_ids(wid), character(0))

  id <- obj$send_message("ECHO", "hello")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_equal(res, "OK")
  expect_error(obj$get_response(id, wid, delete=TRUE),
               "Response missing for workers")

  id <- obj$send_message("EVAL", "1 + 1")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_equal(res, 2)

  id <- obj$send_message("INFO")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_is(res, "worker_info")
  expect_equal(res$worker, wid)

  ## I think I'm not getting the ids correct here.
  id <- obj$send_message("STOP")
  Sys.sleep(.2)
  res <- obj$get_response(id, wid, delete=TRUE)
  expect_equal(res, "BYE")
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
  expect_true(pid_exists(pid))

  ## Better; poll for a change in the status...
  Sys.sleep(.5)
  expect_equal(obj$workers_status(), setNames("BUSY", wid))

  queue_clean(obj$con, "tmpjobs", stop_workers="kill")
  Sys.sleep(.5)

  expect_false(pid_exists(pid))
  test_cleanup()
})

test_that("worker-first startup", {
  test_cleanup()

  wid <- worker_spawn("tmpjobs", "worker.log")
  ## or!
  ##   w <- rrqueue::worker("tmpjobs")

  obs <- observer("tmpjobs")
  expect_equal(obs$workers_list(), wid)
  ## No environments:
  expect_equal(obs$workers_info()[[wid]]$envir, character(0))
  expect_equal(obs$worker_envir(wid), character(0))

  ## startup a queue:
  obj <- queue("tmpjobs", sources="myfuns.R")
  envir_id <- obj$envir_id

  ## This is not updated:
  expect_equal(obs$workers_info()[[1]]$envir, character(0))
  ## But this is (might need a little sleep here)
  Sys.sleep(.1)
  expect_equal(obs$worker_envir(wid), envir_id)

  ## Ask the worker to update the environment:
  obj$send_message("INFO")
  Sys.sleep(.1) # Do a BLPOP on the response queue.
  expect_equal(obs$workers_info()[[1]]$envir, envir_id)

  expect_equal(obj$envirs_list(), envir_id)
  expect_equal(obj$envir_workers(envir_id),
               setNames(TRUE, wid))

  t <- obj$enqueue(sin(1))
  Sys.sleep(.1)
  expect_equal(t$result(), sin(1))

  obj$send_message("STOP")
  Sys.sleep(.1)

  expect_equal(obj$envir_workers(envir_id),
               setNames(TRUE, wid)[-1])
})

test_that("file info", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  expect_equal(existing, character(0))

  filename <- "myfuns.R"
  obj <- queue("tmpjobs", sources=filename)
  dat <- string_to_object(obj$con$HGET(obj$keys$envirs_files, obj$envir_id))

  tmp <- tempfile("rrqueue_")
  files_unpack(obj$files, dat, tmp)
  expect_equal(dir(tmp), filename)
  expect_equal(hash_file(file.path(tmp, filename)),
               hash_file(filename))
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
  expect_equal(dir(tmp), "myfuns.R")
  expect_equal(hash_file(file.path(tmp, "myfuns.R")),
               hash_file("myfuns.R"))

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

  expect_true(file.exists(logfile))

  ## The directory is empty:
  id <- obj$send_message("DIR")
  dat <- obj$get_response(id, wid, wait=5, delete=TRUE)
  expect_equal(dat, empty_named_character())

  ## But we still do have a worker listening on this queue:
  expect_equal(obj$worker_envir(wid), obj$envir_id)
  expect_equal(obj$envir_workers(obj$envir_id), setNames(TRUE, wid))

  ## We can pull files onto the worker too:
  res <- obj$files_pack("myfuns.R")
  dat <- obj$envirs_contents(obj$envir_id)[[1]]$source_files
  id <- obj$send_message("PULL", dat)
  expect_equal(obj$get_response(id, wid, wait=5, delete=TRUE), "OK")

  id <- obj$send_message("DIR")
  dat <- obj$get_response(id, wid, wait=5)
  expect_equal(dat, hash_files("myfuns.R"))

  ## can also exectute commands on the worker:
  id <- obj$send_message("EVAL", "getwd()", wid)
  dat <- obj$get_response(id, wid, wait=5)
  ## Might be platform dependent, unfortunately.
  expect_equal(dat, normalizePath(path))

  id <- obj$send_message("STOP")
  dat <- obj$get_response(id, wid, wait=5)
  expect_equal(dat, "BYE")
})

test_that("complex expressions", {
  obj <- queue("tmpjobs")
  expect_error(obj$enqueue(sin(cos(1))),
               "complex expressions not yet supported")
})

test_that("controlled failures", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  wid <- worker_spawn(obj$queue_name, tempfile())

  t <- obj$enqueue(failure(controlled=TRUE))
  Sys.sleep(.1)
  expect_equal(t$status(), "COMPLETE")
  expect_true(is_error(t$result()))
  expect_false(inherits(t$result(), "WorkerTaskError"))

  t <- obj$enqueue(failure(controlled=FALSE))
  Sys.sleep(.1)
  expect_equal(t$status(), "ERROR")
  expect_true(is_error(t$result()))
  expect_is(t$result(), "WorkerTaskError")

  obj$send_message("STOP")
})

test_that("wait", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  t <- obj$enqueue(slowdouble(0.3))
  expect_error(t$wait(0), "task not returned in time")
  expect_error(t$wait(0.1), "task not returned in time")

  wid <- worker_spawn(obj$queue_name, tempfile())
  expect_equal(t$wait(1), 0.6)
  ## No longer in testthat api :-/
  ## expect_that(t$wait(1), takes_less_than(1))

  t <- obj$enqueue(slowdouble(1))
  expect_error(t$wait(0), "task not returned in time")
  expect_equal(t$wait(2), 2)
  obj$send_message("STOP")
  Sys.sleep(.5)
})

test_that("config", {
  obj <- queue(sources="myfuns.R", config="config.yml")
  dat <- load_config("config.yml")
  expect_equal(obj$queue_name, dat$queue_name)
  cfg <- obj$con$config()
  expect_equal(cfg$host, dat$redis$host)
  expect_equal(cfg$port, dat$redis$port)

  env <- obj$envirs_contents()[[obj$envir_id]]
  expect_equal(env$packages, dat$packages)
  expect_equal(env$sources, dat$sources)
})

test_that("config observer", {
  obj <- observer(config="config.yml")
  dat <- load_config("config.yml")
  expect_equal(obj$queue_name, dat$queue_name)
  cfg <- obj$con$config()
  expect_equal(cfg$host, dat$redis$host)
  expect_equal(cfg$port, dat$redis$port)
})

test_that("refresh_environent", {
  writeLines("f <- function(x) 1", "update.R")
  on.exit(file.remove("update.R"))

  test_cleanup()
  obj <- queue("tmpjobs", sources="update.R")
  envir_id <- obj$envir_id

  wid <- worker_spawn(obj$queue_name, tempfile())

  r1 <- obj$enqueue(f(1))$wait(60)
  expect_equal(r1, 1)

  writeLines("f <- function(x) 2", "update.R")

  r2 <- obj$enqueue(f(2))$wait(60)
  expect_equal(r2, 1)

  expect_message(val <- obj$refresh_environment(),
                 "Initialising environment")
  expect_true(val)
  expect_false(obj$envir_id == envir_id)
  expect_silent(val <- obj$refresh_environment())
  expect_false(val)

  r3 <- obj$enqueue(f(2))$wait(60)
  expect_equal(r3, 2)

  expect_equal(sort(obj$worker_envir(wid)),
               sort(c(envir_id, obj$envir_id)))

  obj$send_message("STOP")
})

test_that("pause", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  wid <- worker_spawn(obj$queue_name, tempfile())

  expect_equal(obj$workers_status(),
               setNames(WORKER_IDLE, wid))

  id <- obj$send_message("PAUSE")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res, "OK")

  expect_equal(obj$workers_status(),
               setNames(WORKER_PAUSED, wid))
  ## Can still read the current environment:
  expect_equal(obj$worker_envir(wid), obj$envir_id)

  t <- obj$enqueue(sin(1))
  ## should have been queued by now if the worker was interested:
  Sys.sleep(.5)
  expect_equal(t$status(), TASK_PENDING)

  id <- obj$send_message("RESUME")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res, "OK")
  Sys.sleep(.5)
  expect_equal(t$status(), TASK_COMPLETE)

  log <- obj$workers_log_tail(wid, 0)

  expect_equal(log$command, c("ALIVE", "ENVIR",
                              "MESSAGE", "RESPONSE",
                              "MESSAGE", "RESPONSE",
                              "TASK_START", "TASK_COMPLETE"))
  expect_equal(log$message, c("", obj$envir_id,
                              "PAUSE", "PAUSE",
                              "RESUME", "RESUME",
                              t$id, t$id))

  ## Pausing twice is fine:
  id <- obj$send_message("PAUSE")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res, "OK")

  id <- obj$send_message("PAUSE")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res, "OK")

  ## Will stop when paused:
  id <- obj$send_message("STOP")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res, "BYE")

  expect_equal(obj$workers_list_exited(), wid)
})

test_that("unknown command", {
  test_cleanup()
  obj <- queue("tmpjobs", sources="myfuns.R")
  wid <- worker_spawn(obj$queue_name, tempfile())

  expect_equal(obj$workers_status(),
               setNames(WORKER_IDLE, wid))

  id <- obj$send_message("XXXX")
  res <- obj$get_response(id, wid, wait=1)
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_null(res$args)

  id <- obj$send_message("YYYY", "ZZZZ")
  res <- obj$get_response(id, wid, wait=1)
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "YYYY")
  expect_equal(res$args, "ZZZZ")

  ## Complex arguments are supported:
  d <- data.frame(a=1, b=2)
  id <- obj$send_message("YYYY", d)
  res <- obj$get_response(id, wid, wait=1)
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "YYYY")
  expect_equal(res$args, d)

  obj$stop_workers()
})
