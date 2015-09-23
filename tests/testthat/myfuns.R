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
