slowdouble <- function(x) {
  Sys.sleep(x)
  x * 2
}

suml <- function(x) {
  x[[1]] + x[[2]]
}

prod2 <- function(a, b) {
  a * b
}

## Simulate a function called for side effects, but without sideeffects...
ret_null <- function(...) {
  Sys.sleep(0.02)
  NULL
}

failure <- function(controlled) {
  if (controlled) {
    try(stop("an expected error"), silent=TRUE)
  } else {
    stop("an unexpected error")
  }
}

## This is a blocking call that skips the R interrupt loop; we won't
## listen for SIGINT during this and the stop request will fail.
block <- function(n) {
  key <- ids::aa(1)(1)
  rrqueue:::redis_connection(NULL)$BLPOP(key, n)
}
