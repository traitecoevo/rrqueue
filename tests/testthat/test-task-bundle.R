context("task_bundle")

test_that("simple", {
  test_cleanup()
  on.exit(test_cleanup())

  existing <- queues()
  ## expect_that(existing, equals(character(0)))

  obj <- queue("tmpjobs", sources="myfuns.R")

  group <- "mygroup"
  x <- obj$task_bundle_get(group)

  expect_that(x, is_a("task_bundle"))
  expect_that(x$ids(), equals(character(0)))
  expect_that(x$groups, equals(group))
  expect_that(x$update_groups(), equals(character(0)))
  expect_that(x$results(), equals(empty_named_list()))
  expect_that(x$wait(), equals(empty_named_list()))

  ## Queue up a job:
  t <- obj$enqueue(sin(1), group=group)

  expect_that(x$update_groups(), equals(t$id))
  expect_that(x$update_groups(), equals(character(0)))

  ids <- t$id
  for (i in 1:3) {
    t <- obj$enqueue(sin(1), group=group)
    ids <- c(ids, t$id)
  }

  expect_that(x$update_groups(), equals(ids[-1]))
  expect_that(x$update_groups(), equals(character(0)))

  expect_that(x$ids(), equals(ids))

  expect_that(x$status(),
              equals(setNames(rep(TASK_PENDING, length(ids)), ids)))

  expect_that(x$results(), throws_error("Tasks not yet completed"))
  expect_that(x$wait(0), throws_error("Tasks not yet completed"))
  expect_that(x$wait(1), throws_error("Exceeded maximum time"))

  expect_that(x$status(),
              equals(setNames(rep(TASK_PENDING, length(ids)), ids)))

  ## Start a worker:
  logfile <- "worker.log"
  Sys.setenv("R_TESTS" = "")
  wid <- worker_spawn(obj$queue_name, logfile)

  Sys.sleep(.5)

  expect_that(x$status(),
              equals(setNames(rep(TASK_COMPLETE, length(ids)), ids)))

  cmp <- setNames(rep(list(sin(1)), length(ids)), ids)
  expect_that(x$results(), equals(cmp))

  ## Set some names and watch them come out too:
  x$names <- letters[1:4]
  x$results()
  cmp <- setNames(rep(list(sin(1)), length(ids)), letters[1:4])
  expect_that(x$results(), equals(cmp))

  ## Add an new task:
  t <- obj$enqueue(sin(1), group=group)
  Sys.sleep(.5)

  ## Nothing has changed in the bundle:
  expect_that(x$results(), equals(cmp))

  id <- x$update_groups()
  expect_that(id, equals(t$id))
  ids <- c(ids, id)

  ## Names have been removed:
  expect_that(x$names, equals(NULL))

  cmp <- setNames(rep(list(sin(1)), length(ids)), ids)
  expect_that(x$results(), equals(cmp))

  t <- obj$enqueue(slowdouble(2), group=group)
  x$update_groups()
  st <- setNames(rep(c(TASK_COMPLETE, TASK_RUNNING), c(length(ids), 1)),
                 c(ids, t$id))
  Sys.sleep(.2)
  expect_that(x$status(), equals(st))
  r <- x$wait(3)

  cmp <- c(cmp, setNames(list(4), t$id))
  expect_that(r, equals(cmp))
  expect_that(x$status(),
              equals(setNames(rep(TASK_COMPLETE, length(r)), names(r))))

  expect_that(x$wait1(1), equals(NULL))
  expect_that(x$wait1(1), takes_less_than(1))

  t1 <- obj$enqueue(slowdouble(1), group=group)
  t2 <- obj$enqueue(slowdouble(2), group=group)
  x$update_groups()
  res <- x$wait1(60)
  expect_that(res[[1]], equals(t1$id))
  expect_that(res[[2]], equals(2))
  res <- x$wait1(60)
  expect_that(res[[1]], equals(t2$id))
  expect_that(res[[2]], equals(4))
  expect_that(x$wait1(), is_null())

  t3 <- obj$enqueue(slowdouble(3), group=group)
  x$update_groups()
  res0 <- x$wait1(1)
  expect_that(res0, is_null())
  res <- x$wait1(10)
  expect_that(res, equals(list(id=t3$id, result=t3$result())))

  ## Get the bundle again:
  y <- obj$task_bundle_get(group)
  expect_that(y$ids(),     equals(x$ids()))
  expect_that(y$status(),  equals(x$status()))
  expect_that(y$results(), equals(x$results()))

  obj$stop_workers(wid)
})
