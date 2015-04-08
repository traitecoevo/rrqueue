context("object cache")

test_that("object_cache", {
  key <- "tmp:objects"
  obj <- object_cache(NULL, "tmp:objects")

  con <- redis_connection(NULL)
  object_cache_cleanup(con, key)
  on.exit(object_cache_cleanup(con, key))

  expect_that(obj$list(), equals(character(0)))
  val <- 1:10
  obj$set("foo", val)
  expect_that(obj$get("foo"), equals(val))
  rm(obj)

  obj2 <- object_cache(NULL, "tmp:objects")
  expect_that(ls(obj2$cache), equals(character(0)))
  expect_that(obj2$get("foo"), equals(val))
  expect_that(ls(obj2$cache),
              equals(hash_string(object_to_string(val))))
  expect_that(obj2$cache[[ls(obj2$cache)]], is_identical_to(val))
})
