context("task_bundle")

test_that("simple", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  ## expect_equal(existing, character(0))

  obj <- queue("tmpjobs", sources="myfuns.R")

  group <- "mygroup"
  x <- obj$task_bundle_get(group)

  expect_is(x, "task_bundle")
  expect_equal(x$ids(), character(0))
  expect_equal(x$groups, group)
  expect_equal(x$update_groups(), character(0))
  expect_equal(x$results(), empty_named_list())
  expect_equal(x$wait(), empty_named_list())
  expect_equal(as.list(x$overview()),
               list(PENDING=0, RUNNING=0, COMPLETE=0, ERROR=0))

  ## Queue up a job:
  t <- obj$enqueue(sin(1), group=group)

  expect_equal(x$update_groups(), t$id)
  expect_equal(x$update_groups(), character(0))

  ids <- t$id
  for (i in 1:3) {
    t <- obj$enqueue(sin(1), group=group)
    ids <- c(ids, t$id)
  }

  expect_equal(x$update_groups(), ids[-1])
  expect_equal(x$update_groups(), character(0))

  expect_equal(x$ids(), ids)

  expect_equal(x$status(),
               setNames(rep(TASK_PENDING, length(ids)), ids))

  expect_error(x$results(), "Tasks not yet completed")
  expect_error(x$wait(0), "Tasks not yet completed")
  expect_error(x$wait(1), "Exceeded maximum time")

  expect_equal(x$status(),
               setNames(rep(TASK_PENDING, length(ids)), ids))

  ## Start a worker:
  logfile <- "worker.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  Sys.sleep(.5)

  expect_equal(x$status(),
               setNames(rep(TASK_COMPLETE, length(ids)), ids))

  cmp <- setNames(rep(list(sin(1)), length(ids)), ids)
  expect_equal(x$results(), cmp)

  ## Set some names and watch them come out too:
  x$names <- letters[1:4]
  x$results()
  cmp <- setNames(rep(list(sin(1)), length(ids)), letters[1:4])
  expect_equal(x$results(), cmp)

  ## Add an new task:
  t <- obj$enqueue(sin(1), group=group)
  Sys.sleep(.5)

  ## Nothing has changed in the bundle:
  expect_equal(x$results(), cmp)

  id <- x$update_groups()
  expect_equal(id, t$id)
  ids <- c(ids, id)

  ## Names have been removed:
  expect_null(x$names)

  cmp <- setNames(rep(list(sin(1)), length(ids)), ids)
  expect_equal(x$results(), cmp)

  t <- obj$enqueue(slowdouble(2), group=group)
  x$update_groups()
  st <- setNames(rep(c(TASK_COMPLETE, TASK_RUNNING), c(length(ids), 1)),
                 c(ids, t$id))
  Sys.sleep(.2)
  expect_equal(x$status(), st)
  r <- x$wait(3)

  cmp <- c(cmp, setNames(list(4), t$id))
  expect_equal(r, cmp)
  expect_equal(x$status(),
               setNames(rep(TASK_COMPLETE, length(r)), names(r)))

  expect_null(x$wait1(1))
  ## No update for this one in new testthat, being removed because who
  ## needs a stable API.
  ##   expect_that(x$wait1(1), takes_less_than(1))

  t1 <- obj$enqueue(slowdouble(1), group=group)
  t2 <- obj$enqueue(slowdouble(2), group=group)
  x$update_groups()
  res <- x$wait1(60)
  expect_equal(res[[1]], t1$id)
  expect_equal(res[[2]], 2)
  res <- x$wait1(60)
  expect_equal(res[[1]], t2$id)
  expect_equal(res[[2]], 4)
  expect_null(x$wait1())

  t3 <- obj$enqueue(slowdouble(3), group=group)
  x$update_groups()
  res0 <- x$wait1(1)
  expect_null(res0)
  res <- x$wait1(10)
  expect_equal(res, list(id=t3$id, result=t3$result()))

  expect_equal(as.list(x$overview()),
               list(PENDING=0, RUNNING=0, COMPLETE=9, ERROR=0))

  ## Yeah, this is not going to work.
  xt <- x$times()
  expect_is(xt, "data.frame")
  cols <- c("task_id", "submitted", "started", "finished",
            "waiting", "running", "idle")
  expect_equal(names(xt), cols)
  cols <- setdiff(cols, "idle")
  expect_equal(xt[cols], obj$tasks_times(x$ids())[cols])

  ## Get the bundle again:
  y <- obj$task_bundle_get(group)
  expect_equal(y$ids(),     x$ids())
  expect_equal(y$status(),  x$status())
  expect_equal(y$results(), x$results())

  obj$stop_workers(wid)
})
