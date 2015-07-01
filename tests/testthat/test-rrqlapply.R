context("rrqlapply")

## The first "high" level thing; a really basic mclapply like clone.
## TODO: test passing in an unknown function
test_that("Basic use", {
  test_cleanup()
  on.exit(test_cleanup())

  obj <- queue("tmpjobs", sources="myfuns.R")
  x <- sample(1:10, 20, replace=TRUE)
  rrql <- rrqlapply_submit(x, "sin", obj)
  monitor_status(obj)

  tasks <- names(rrql$tasks)
  expect_that(obj$tasks_status(tasks),
              equals(setNames(rep(TASK_PENDING, length(x)), tasks)))

  wid <- worker_spawn(obj$queue_name, "rrqlapply.log")

  ## TODO:
  Sys.sleep(1.0)
  expect_that(obj$tasks_status(tasks),
              equals(setNames(rep(TASK_COMPLETE, length(x)), tasks)))

  res <- rrqlapply_results(rrql, progress_bar=FALSE)
  cmp <- lapply(x, sin)
  expect_that(res, equals(cmp, tolerance=1e-15))

  res <- rrqlapply_results(rrql, delete_tasks=TRUE, progress_bar=FALSE)

  ## Cleanup has happened
  expect_that(obj$tasks_status(tasks),
              equals(setNames(rep(TASK_MISSING, length(x)), tasks)))

  res <- rrqlapply(x, "sin", obj, progress_bar=FALSE)
  expect_that(res, equals(cmp, tolerance=1e-15))

  obj$send_message("STOP")
})

test_that("bulk", {
  x <- expand.grid(a=1:4, b=runif(3))

  obj <- queue("tmpjobs", sources="myfuns.R")

  ## Serial versions:
  cmp_sum  <- lapply(df_to_list(x), suml)
  cmp_prod <- lapply(df_to_list(x), function(el) prod2(el$a, el$b))

  wid <- worker_spawn(obj$queue_name, "rrqlapply.log")

  res <- enqueue_bulk(x, suml, obj)
  expect_that(res, equals(cmp_sum))

  res <- enqueue_bulk(x, prod2, obj, do.call=TRUE)
  expect_that(res, equals(cmp_prod))

  obj$send_message("STOP")
})
