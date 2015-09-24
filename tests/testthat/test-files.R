context("files")

test_that("io", {
  filename <- "myfuns.R"
  str <- read_file_to_string(filename)
  expect_that(hash_string(str), equals(hash_file(filename)))

  tmp <- tempfile("rrqueue_")
  write_string_to_file(str, tmp)
  expect_that(hash_file(tmp), equals(hash_file(filename)))
  expect_that(readLines(tmp), not(gives_warning()))
})

test_that("files", {
  test_cleanup()
  con <- redis_connection(NULL)
  prefix <- "tmpjobs:files:"
  cache <- file_cache(prefix, con)

  expect_that(cache$list(), equals(character(0)))

  obj <- files_pack(cache)
  expect_that(obj, equals(structure(character(0), class="files_pack")))

  obj <- files_pack(cache, "myfuns.R")
  expect_that(length(obj), equals(1))
  expect_that(names(obj), equals("myfuns.R"))
  expect_that(obj[["myfuns.R"]], equals(hash_file("myfuns.R")))

  tmp <- tempfile("rrqueue_")
  files_unpack(cache, obj, tmp)
  path <- file.path(tmp, "myfuns.R")
  expect_that(file.exists(path), is_true())
  expect_that(hash_file(path), equals(obj[["myfuns.R"]]))
})
