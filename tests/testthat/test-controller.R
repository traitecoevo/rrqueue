context("controller")

test_that("controller", {
  test_cleanup()
  on.exit(test_cleanup())

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_that(obj$con, is_a("hiredis"))
  expect_that(obj$name, equals("tmpjobs"))

  keys <- rrqueue_keys(obj$name)

  con <- obj$con
  expect_that(sort(as.character(con$KEYS("tmpjobs*"))),
              equals(sort(c(keys$packages, keys$sources))))

  expect_that(string_to_object(con$GET(keys$packages)), equals(NULL))
  expect_that(string_to_object(con$GET(keys$sources)), equals("myfuns.R"))

  ## Queue two jobs:
  id1 <- obj$enqueue(sin(1))
  id2 <- obj$enqueue(sin(2))

  expect_that(id1, equals("1"))
  expect_that(id2, equals("2"))

  expect_that(sort(as.character(con$KEYS("tmpjobs*"))),
              equals(sort(c(keys$packages, keys$sources,
                            keys$tasks, keys$tasks_counter,
                            keys$tasks_id, keys$tasks_status))))

  expect_that(con$TYPE(keys$tasks_id),      equals("list"))
  expect_that(con$TYPE(keys$tasks),         equals("hash"))
  expect_that(con$TYPE(keys$tasks_counter), equals("string"))
  expect_that(con$TYPE(keys$tasks_status),  equals("hash"))

  ids <- con$LRANGE(keys$tasks_id, 0, -1)
  expect_that(ids, equals(list("1", "2")))
  ids <- as.character(ids)

  tasks <- unname(from_redis_hash(con, keys$tasks)[as.character(ids)])
  e <- new.env(parent=.GlobalEnv)
  tasks <- lapply(tasks, restore_expression, e, obj$objects)
  expect_that(ls(e), equals(character(0)))
  expect_that(tasks, equals(list(quote(sin(1)), quote(sin(2)))))

  expect_that(con$GET(keys$tasks_counter), equals("2"))

  status <- from_redis_hash(con, keys$tasks_status)
  expect_that(unname(status[as.character(ids)]),
              equals(rep(as.character(TASK_PENDING), length(ids))))

  expect_that(obj$tasks(), equals(from_redis_hash(con, keys$tasks)))

  expect_that(obj$tasks_drop(c(id1, id2)), equals(c(TRUE, TRUE)))
  expect_that(obj$tasks_drop(c(id1, id2)), equals(c(FALSE, FALSE)))
  expect_that(obj$tasks(), equals(empty_named_character()))
  expect_that(con$LRANGE(keys$tasks_id, 0, -1), equals(list()))

  ## This needs to send output to a file and not to stdout!
  logfile <- "worker.log"
  ## See: https://github.com/hadley/testthat/issues/144
  Sys.setenv("R_TESTS" = "")
  wid <- rrqueue_worker_spawn(obj$name, logfile)

  expect_that(obj$n_workers(), equals(1))

  expect_that(con$TYPE(keys$workers_status), equals("hash"))
  expect_that(con$TYPE(keys$workers), equals("set"))

  w <- obj$workers()
  expect_that(length(w), equals(1L))
  expect_that(w, is_identical_to(wid))

  ww <- parse_worker_name(w)
  expect_that(ww$host, equals(Sys.info()[["nodename"]]))

  expect_that(from_redis_hash(con, keys$workers_status),
              equals(structure(as.character(WORKER_IDLE),
                               names=w[[1]])))

  ## OK, submit a trivial job
  id <- obj$enqueue(sin(1))
  Sys.sleep(0.5)

  ## TODO: Fix $tasks() here
  expect_that(restore_expression(obj$tasks()[["3"]], e, obj$objects),
              equals(quote(sin(1))))
  expect_that(ls(e), equals(character(0)))

  expect_that(obj$tasks_status(), equals(c("3"=TASK_COMPLETE)))
  expect_that(obj$tasks_collect("3"), equals(sin(1)))

  ## Another, using arguments:
  x <- 1
  e <- environment()
  id <- obj$enqueue(sin(x), e)

  ## TODO: factor out the mangling here.
  expect_that(obj$objects$list(),
              equals(sprintf(".%s:x", id)))
  expect_that(restore_expression(obj$tasks()[[id]], e, obj$objects),
              equals(quote(sin(x))))

  expect_that(obj$tasks_status()[[id]], equals(TASK_COMPLETE))
  expect_that(obj$tasks_collect("4"), equals(sin(x)))

  ## TODO:
  ## on job removal, clean up objects that were created.  That's easy
  ## enough to set up with enqueue, and then trigger later.

  ## TODO: get statistics off the workers about completed jobs
  ## perhaps?  That can be stored within the worker rather than the
  ## database I guess?

  obj$send_message("STOP", w)
  Sys.sleep(.5)
  expect_that(obj$n_workers(), equals(0))
  expect_that(obj$workers(), equals(character(0)))

  ## TODO: cleanup properly.
})
