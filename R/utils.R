banner <- function(text) {
  pkgs <- .packages(TRUE)
  if ("crayon" %in% pkgs) {
    style <- crayon::make_style("magenta")
    f <- function(x) message(style(x))
  } else {
    f <- message
  }
  if ("rfiglet" %in% pkgs) {
    text <- as.character(rfiglet::figlet(text, "slant"))
  }
  f(text)
}

##' @importFrom digest digest
hash_string <- function(x) {
  digest::digest(x, serialize=FALSE)
}

is_error <- function(x) {
  inherits(x, "try-error")
}

is_directory <- function(path) {
  file.info(path)$isdir
}

is_terminal <- function() {
  if (!isatty(stdout())) {
    return(FALSE)
  }
  if (.Platform$OS.type == "windows") {
    return(FALSE)
  }
  if (Sys.getenv("TERM") == "dumb") {
    return(FALSE)
  }
  !is_error(try(system("tput colors", intern=TRUE), silent=TRUE))
}

vcapply <- function(X, FUN, ...) {
  vapply(X, FUN, character(1), ...)
}
viapply <- function(X, FUN, ...) {
  vapply(X, FUN, integer(1), ...)
}
vlapply <- function(X, FUN, ...) {
  vapply(X, FUN, logical(1), ...)
}
