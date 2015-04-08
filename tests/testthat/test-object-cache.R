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
  hash <- con$HGET(obj$keys$hash, "foo")
  expect_that(hash, equals(hash_string(object_to_string(val))))
  expect_that(con$HGET(obj$keys$count, hash), equals("1"))
  expect_that(con$HGET(obj$keys$data,  hash),
              equals(object_to_string(val)))

  rm(obj)

  obj2 <- object_cache(NULL, "tmp:objects")
  expect_that(ls(obj2$envir), equals(character(0)))
  expect_that(obj2$get("foo"), equals(val))
  expect_that(ls(obj2$envir),
              equals(hash_string(object_to_string(val))))
  expect_that(obj2$envir[[ls(obj2$envir)]], is_identical_to(val))

  ## Here is the data hash:
  expect_that(con$HKEYS(obj2$keys$data),
              equals(list(hash)))

  ## Now, make a *second* object pointing that the same thing.
  obj2$set("bar", val)

  ## Nothing extra here:
  expect_that(con$HKEYS(obj2$keys$data),
              equals(list(hash)))
  expect_that(sort(obj2$list()), equals(sort(c("foo", "bar"))))
  ## Counter reflects that we have two things here:
  expect_that(con$HGET(obj2$keys$count, hash), equals("2"))

  obj2$drop("foo")
  expect_that(con$HGET(obj2$keys$count, hash), equals("1"))
  obj2$drop("bar")
  expect_that(con$HGET(obj2$keys$count, hash), equals(NULL))
  expect_that(con$HKEYS(obj2$keys$data), equals(list()))
  expect_that(con$HLEN(obj2$keys$data),  equals(0L))
  expect_that(con$HLEN(obj2$keys$count), equals(0L))
  expect_that(ls(obj2$envir), equals(character(0)))
})
