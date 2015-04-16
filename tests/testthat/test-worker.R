context("worker")

test_that("worker_status_time - nonexistant worker", {
  obs <- observer("tmpjobs")
  name <- "no such worker"
  t <- obs$workers_status_time(name)
  expect_that(t, is_a("data.frame"))
  expect_that(nrow(t), equals(1))
  expect_that(t, equals(data.frame(worker_id=name,
                                   expire_max=NA_real_,
                                   expire=-2.0,
                                   last_seen=NA_real_,
                                   last_action=NA_real_,
                                   stringsAsFactors=FALSE)))
})
