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

install_script <- function(contents, dest, overwrite=FALSE) {
  destination_directory <- dirname(dest)
  if (!file.exists(destination_directory) ||
      !is_directory(destination_directory)) {
    stop("Destination must be an existing directory")
  }

  if (file.exists(dest) && !overwrite) {
    stop(sprintf("File %s already exists", dest))
  }
  writeLines(contents, dest)
  Sys.chmod(dest, "0755")
}

docopt_parse <- function(...) {
  oo <- options(warnPartialMatchArgs=FALSE)
  if (isTRUE(oo$warnPartialMatchArgs)) {
    on.exit(options(oo))
  }
  docopt::docopt(...)
}

lstrip <- function(x) {
  sub("^\\s+", "", x, perl=TRUE)
}
