context("function matching")

## NOTE: this process has the potential to be quite slow; I don't
## think we want to do it for everything over and over.  But let's
## start here and work back to get

test_that("has_namespace", {
  expect_true(has_namespace("foo::bar"))
  expect_true(has_namespace("::bar"))
  expect_true(has_namespace("foo::"))
  ## false positive:
  expect_true(has_namespace("foo::bar::baz"))
  expect_false(has_namespace("foo"))
  expect_false(has_namespace(":foo:"))
})

test_that("split_namespace", {
  expect_equal(split_namespace("foo::bar"), c("foo", "bar"))
  expect_equal(split_namespace("::bar"), c("", "bar"))
  ## Odd behaviour of strsplit():

  msg <- "Not a namespace-qualified variable"
  expect_error(split_namespace("foo::"), msg)
  ## false positive: - should error here?
  expect_error(split_namespace("foo::bar::baz"), msg)
  expect_error(split_namespace("foo"), msg)
  expect_error(split_namespace(":foo:"), msg)
})

test_that("match_fun_name", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)

  expect_equal(match_fun_name("rrqueue::worker", e),
               c("rrqueue", "worker"))
  expect_error(match_fun_name("rrqueuex::worker", e),
               "Did not find function")
  expect_error(match_fun_name("rrqueue::workerx", e),
               "Did not find function")

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_equal(match_fun_name("slowdouble", e), cmp)
  expect_error(match_fun_name("slowdouble_no_such", e),
               "Did not find function")
})

test_that("match_fun_symbol", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)
  expect_equal(match_fun_symbol(quote(worker), e),
               c("rrqueue", "worker"))
  expect_error(match_fun_symbol(quote(workerx), e),
               "Did not find function")

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_equal(match_fun_symbol(quote(slowdouble), e), cmp)
  expect_error(match_fun_symbol(quote(slowdouble_no_such), e),
               "Did not find function")
})

test_that("match_fun_value", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)
  fun <- e$slowdouble

  expect_equal(match_fun_value(worker, e),
               c("rrqueue", "worker"))
  ww <- worker
  expect_equal(match_fun_value(ww, e),
               c("rrqueue", "worker"))
  expect_equal(match_fun_value(get("worker"), e),
               c("rrqueue", "worker"))

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_equal(match_fun_value(fun, e), cmp)

  e2 <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e2)
  expect_false(identical(e2$slowdouble, e$slowdouble))

  expect_error(match_fun_value(worker, baseenv()),
               "Did not find function")
  expect_error(match_fun_value(fun, e2),
               "Did not find function")
})

test_that("match_fun", {
  env <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", env)

  fun <- env$slowdouble
  cmp <- structure(c("", "slowdouble"), envir=env)

  expect_equal(match_fun("slowdouble", env), cmp)
  expect_equal(match_fun(get("slowdouble", env), env), cmp)
  expect_equal(match_fun(fun, env), cmp)

  fun <- rrqueue::worker
  cmp <- c("rrqueue", "worker")

  expect_equal(match_fun("rrqueue::worker", env), cmp)
  expect_equal(match_fun("worker", env), cmp)
  expect_equal(match_fun(rrqueue::worker, env), cmp)
  expect_equal(match_fun(get("worker", env), env), cmp)
  expect_equal(match_fun(fun, env), cmp)
})

test_that("match_fun_rrqueue", {
  env1 <- new.env(parent=parent.env(.GlobalEnv))
  env2 <- new.env(parent=parent.env(.GlobalEnv))
  env3 <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", env1)
  env3$slowdouble <- function(x) x * 2

  fun <- env1$slowdouble
  cmp <- structure(c("", "slowdouble"), envir=env1)

  expect_equal(match_fun_rrqueue("slowdouble", env1, env1), cmp)
  expect_error(match_fun_rrqueue("slowdouble", env1, env2),
               "Function not found in rrqueue environment")
  expect_error(match_fun_rrqueue("slowdouble", env1, env3),
               "Function found in given and rrqueue")

  expect_equal(match_fun_rrqueue(fun, env1, env1), cmp)
  expect_error(match_fun_rrqueue(fun, env1, env2),
               "Function not found in rrqueue environment")
  expect_error(match_fun_rrqueue(fun, env1, env3),
               "Function found in given and rrqueue")
})
