##' @importFrom crayon make_style
banner <- function(text) {
  pkgs <- .packages(TRUE)
  style <- crayon::make_style(random_colour())
  f <- function(x) message(style(x))
  if ("rfiglet" %in% pkgs) {
    text <- as.character(rfiglet::figlet(text, "slant"))
  }
  f(text)
}

##' @importFrom digest digest
hash_string <- function(x) {
  digest::digest(x, serialize=FALSE)
}

hash_file <- function(x) {
  setNames(digest::digest(file=x), x)
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

## Source a file (using sys.source) and record all files that file
## sources (only via source and sys.source, ignoring file connections,
## assuming files don't change, etc, etc).
sys_source <- function(...) {
  collector <- function(...) {
    e <- parent.frame(2)
    if (exists("file", e, inherits=FALSE)) {
      file <- get("file", e, inherits=FALSE)
      if (is.character(file)) {
        ## TODO: need to deal with the case where source(...,
        ## chdir=TRUE) was used and the path has changed; in that case
        ## we're going to need to work out where the file is relative
        ## to the current directory, which requires pathr to work.
        ##
        ## If we *do* do this, then the create_environment function
        ## needs to take care of that bookkeeping.
        dat <<- c(dat, hash_file(file))
      } else {
        warning("non-file source detected")
      }
    } else {
      warning("source detection failed")
    }
    dat
  }
  dat <- character(0)

  suppressMessages({
    trace(base::source,     function(...) collector(), print=FALSE)
    trace(base::sys.source, function(...) collector(), print=FALSE)
  })
  on.exit({
    suppressMessages({
      untrace(base::source)
      untrace(base::sys.source)
    })
  })
  sys.source(...)
  dat
}

random_colour <- function(n=1) {
  rgb(runif(n), runif(n), runif(n))
}

strrep <- function (str, n) {
  paste(rep_len(str, n), collapse = "")
}
