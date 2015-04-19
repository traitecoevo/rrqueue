context("function matching")

## NOTE: this process has the potential to be quite slow; I don't
## think we want to do it for everything over and over.  But let's
## start here and work back to get

test_that("has_namespace", {
  expect_that(has_namespace("foo::bar"), is_true())
  expect_that(has_namespace("::bar"), is_true())
  expect_that(has_namespace("foo::"), is_true())
  ## false positive:
  expect_that(has_namespace("foo::bar::baz"), is_true())
  expect_that(has_namespace("foo"), is_false())
  expect_that(has_namespace(":foo:"), is_false())
})

test_that("split_namespace", {
  expect_that(split_namespace("foo::bar"), equals(c("foo", "bar")))
  expect_that(split_namespace("::bar"), equals(c("", "bar")))
  ## Odd behaviour of strsplit():

  throws <- throws_error("Not a namespace-qualified variable")
  expect_that(split_namespace("foo::"), throws)
  ## false positive: - should error here?
  expect_that(split_namespace("foo::bar::baz"), throws)
  expect_that(split_namespace("foo"), throws)
  expect_that(split_namespace(":foo:"), throws)
})

test_that("match_fun_name", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)

  expect_that(match_fun_name("rrqueue::worker", e),
              equals(c("rrqueue", "worker")))
  expect_that(match_fun_name("rrqueuex::worker", e),
                           throws_error("Did not find function"))
  expect_that(match_fun_name("rrqueue::workerx", e),
              throws_error("Did not find function"))

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_that(match_fun_name("slowdouble", e), equals(cmp))
  expect_that(match_fun_name("slowdouble_no_such", e),
              throws_error("Did not find function"))
})

test_that("match_fun_symbol", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)
  expect_that(match_fun_symbol(quote(worker), e),
              equals(c("rrqueue", "worker")))
  expect_that(match_fun_symbol(quote(workerx), e),
              throws_error("Did not find function"))

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_that(match_fun_symbol(quote(slowdouble), e),
              equals(cmp))
  expect_that(match_fun_symbol(quote(slowdouble_no_such), e),
              throws_error("Did not find function"))
})

test_that("match_fun_value", {
  e <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e)
  fun <- e$slowdouble

  expect_that(match_fun_value(worker, e),
              equals(c("rrqueue", "worker")))
  ww <- worker
  expect_that(match_fun_value(ww, e),
              equals(c("rrqueue", "worker")))
  expect_that(match_fun_value(get("worker"), e),
              equals(c("rrqueue", "worker")))

  cmp <- structure(c("", "slowdouble"), envir=e)
  expect_that(match_fun_value(fun, e),
              equals(cmp))

  e2 <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", e2)
  expect_that(e2$slowdouble, not(is_identical_to(e$slowdouble)))

  expect_that(match_fun_value(worker, baseenv()),
              throws_error("Did not find function"))
  expect_that(match_fun_value(fun, e2),
              throws_error("Did not find function"))
})

test_that("match_fun", {
  env <- new.env(parent=parent.env(.GlobalEnv))
  source("myfuns.R", env)

  fun <- env$slowdouble
  cmp <- structure(c("", "slowdouble"), envir=env)

  expect_that(match_fun("slowdouble", env), equals(cmp))
  expect_that(match_fun(get("slowdouble", env), env), equals(cmp))
  expect_that(match_fun(fun, env), equals(cmp))

  fun <- rrqueue::worker
  cmp <- c("rrqueue", "worker")

  expect_that(match_fun("rrqueue::worker", env), equals(cmp))
  expect_that(match_fun("worker", env), equals(cmp))
  expect_that(match_fun(rrqueue::worker, env), equals(cmp))
  expect_that(match_fun(get("worker", env), env), equals(cmp))
  expect_that(match_fun(fun, env), equals(cmp))
})
