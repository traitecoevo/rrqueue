context("rrqlapply")

## The first "high" level thing; a really basic mclapply like clone.
## TODO: test passing in an unknown function
test_that("Basic use", {
  test_cleanup()
  on.exit(test_cleanup())

  obj <- queue("tmpjobs", sources="myfuns.R")
  expect_equal(obj$tasks_groups_list(), character(0))
  x <- sample(1:10, 20, replace=TRUE)
  rrql <- rrqlapply_submit(x, "sin", obj)
  expect_is(rrql, "task_bundle")

  grp <- obj$tasks_groups_list()
  expect_equal(length(grp), 1L)

  expect_equal(grp, rrql$groups)
  expect_equal(obj$tasks_in_groups(grp), rrql$ids())
  expect_equal(rrql$names, names(x))

  tmp <- task_bundle_get(obj, groups=grp)
  expect_is(tmp, "task_bundle")
  expect_equal(tmp$groups, grp)
  expect_equal(tmp$names, NULL)
  expect_equal(tmp$key_complete, rrql$key_complete)

  ## TODO: This would be nice to do filtering by jobs in the bundle...
  monitor_status(obj)

  task_ids <- rrql$ids()
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_PENDING, length(x)), task_ids))

  wid <- worker_spawn(obj$queue_name, "rrqlapply.log")
  ## w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  ## TODO:
  Sys.sleep(2)
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_COMPLETE, length(x)), task_ids))

  res <- rrql$wait()
  cmp <- setNames(lapply(x, sin), rrql$ids())
  expect_equal(res, cmp, tolerance=1e-15)

  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_COMPLETE, length(x)), task_ids))

  rrql$delete_tasks()

  ## Cleanup has happened
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_MISSING, length(x)), task_ids))

  res <- rrqlapply(x, "sin", obj, progress_bar=FALSE)
  ## NOTE: hardcoded name here:
  expect_equal(res, setNames(cmp, 21:40), tolerance=1e-15)

  obj$send_message("STOP")
})

test_that("null return", {
  test_cleanup()
  on.exit(test_cleanup())

  obj <- queue("tmpjobs", sources="myfuns.R")
  x <- sample(1:10, 20, replace=TRUE)
  rrql <- rrqlapply_submit(x, "ret_null", obj)
  monitor_status(obj)

  task_ids <- rrql$ids()
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_PENDING, length(x)), task_ids))

  wid <- worker_spawn(obj$queue_name, "rrqlapply.log")
  ## w <- rrqueue::worker("tmpjobs", heartbeat_period=10)

  ## TODO:
  Sys.sleep(1.0)
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_COMPLETE, length(x)), task_ids))

  res <- rrql$wait(progress_bar=FALSE)
  cmp <- named_list(rrql$ids())
  expect_equal(res, cmp)

  ## And again:
  expect_equal(rrql$wait(progress_bar=FALSE), cmp)
  rrql$delete_tasks()

  ## Cleanup has happened
  expect_equal(obj$tasks_status(task_ids),
               setNames(rep(TASK_MISSING, length(x)), task_ids))

  res <- rrqlapply(x, "ret_null", obj, progress_bar=FALSE)
  expect_equal(res, setNames(cmp, 21:40))

  obj$send_message("STOP")
})

test_that("bulk", {
  test_cleanup()
  x <- expand.grid(a=1:4, b=runif(3))

  obj <- queue("tmpjobs", sources="myfuns.R")

  ## Serial versions:
  cmp_sum  <- lapply(df_to_list(x), suml)
  cmp_prod <- lapply(df_to_list(x), function(el) prod2(el$a, el$b))

  wid <- worker_spawn(obj$queue_name, "rrqlapply.log")

  res <- enqueue_bulk_submit(x, suml, obj)
  expect_is(res$groups, "character")

  ans <- res$wait()
  expect_equal(ans, setNames(cmp_sum, res$ids()))

  ## All at once:
  res <- enqueue_bulk(x, prod2, obj, do.call=TRUE)
  expect_equal(unname(res), cmp_prod)

  obj$send_message("STOP")
  test_cleanup()
})
