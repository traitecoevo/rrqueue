context("files")

test_that("io", {
  filename <- "myfuns.R"
  str <- read_file_to_string(filename)
  expect_equal(hash_string(str), hash_file(filename))

  tmp <- tempfile("rrqueue_")
  write_string_to_file(str, tmp)
  expect_equal(hash_file(tmp), hash_file(filename))
  expect_silent(readLines(tmp))
})

test_that("files", {
  test_cleanup()
  con <- redis_connection(NULL)
  prefix <- "tmpjobs:files:"
  cache <- file_cache(prefix, con)

  expect_equal(cache$list(), character(0))

  obj <- files_pack(cache)
  expect_equal(obj, structure(character(0), class="files_pack"))

  obj <- files_pack(cache, "myfuns.R")
  expect_equal(length(obj), 1)
  expect_equal(names(obj), "myfuns.R")
  expect_equal(obj[["myfuns.R"]], hash_file("myfuns.R"))

  tmp <- tempfile("rrqueue_")
  files_unpack(cache, obj, tmp)
  path <- file.path(tmp, "myfuns.R")
  expect_true(file.exists(path))
  expect_equal(hash_file(path), obj[["myfuns.R"]])
})
