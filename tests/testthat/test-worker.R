context("worker")

test_that("workers_times - nonexistant worker", {
  obs <- observer("tmpjobs")
  name <- "no such worker"
  t <- obs$workers_times(name)
  expect_that(t, is_a("data.frame"))
  expect_that(nrow(t), equals(1))
  expect_that(t, equals(data.frame(worker_id=name,
                                   expire_max=NA_real_,
                                   expire=-2.0,
                                   last_seen=NA_real_,
                                   last_action=NA_real_,
                                   stringsAsFactors=FALSE)))
})

test_that("workers_times - no workers", {
  obs <- observer("tmpjobs")
  t <- obs$workers_times()
  expect_that(t, is_a("data.frame"))
  expect_that(nrow(t), equals(0))
  expect_that(t, equals(data.frame(worker_id=character(0),
                                   expire_max=numeric(0),
                                   expire=numeric(0),
                                   last_seen=numeric(0),
                                   last_action=numeric(0),
                                   stringsAsFactors=FALSE)))

  expect_that(obs$workers_list(), equals(character(0)))
  expect_that(obs$workers_status(), equals(empty_named_character()))

  log <- obs$workers_log_tail()
  expect_that(log, equals(data.frame(worker_id=character(0),
                                     time=character(0),
                                     command=character(0),
                                     message=character(0),
                                     stringsAsFactors=FALSE)))
})
