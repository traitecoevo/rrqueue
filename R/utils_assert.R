## Type
assert_inherits <- function(x, what, name=deparse(substitute(x))) {
  if (!inherits(x, what)) {
    stop(sprintf("%s must be a %s", name,
                 paste(what, collapse=" / ")), call.=FALSE)
  }
}
assert_character <- function(x, name=deparse(substitute(x))) {
  if (!is.character(x)) {
    stop(sprintf("%s must be character", name), call.=FALSE)
  }
}

assert_character_or_null <- function(x, name=deparse(substitute(x))) {
  if (!is.null(x)) {
    assert_character(x, name)
  }
}

## Length
assert_scalar <- function(x, name=deparse(substitute(x))) {
  if (length(x) != 1) {
    stop(sprintf("%s must be a scalar", name), call.=FALSE)
  }
}

## Compound:
assert_scalar_character <- function(x, name=deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_character(x, name)
}

assert_integer_like <- function(x, name=deparse(substitute(x))) {
  if (!isTRUE(all.equal(as.integer(x), x))) {
    stop(sprintf("%s is not integer like", name))
  }
}
