hash_string <- function(x) {
  digest::digest(x, serialize=FALSE)
}

hash_file <- function(x) {
  digest::digest(file=x)
}

hash_files <- function(x) {
  setNames(vcapply(x, hash_file), x)
}

compare_hash <- function(x) {
  if (length(x) == 0L) {
    TRUE
  } else {
    files <- names(x)
    all(file.exists(files)) && identical(hash_files(files), x)
  }
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
vnapply <- function(X, FUN, ...) {
  vapply(X, FUN, numeric(1), ...)
}
viapply <- function(X, FUN, ...) {
  vapply(X, FUN, integer(1), ...)
}
vlapply <- function(X, FUN, ...) {
  vapply(X, FUN, logical(1), ...)
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
rstrip <- function(x) {
  sub("\\s+$", "", x, perl=TRUE)
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
        ##
        ## NOTE: using hash_files(), not hash_file(), as the latter
        ## adds names.
        dat <<- c(dat, hash_files(file))
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

Sys_kill <- function(pid, signal=NULL) {
  system2("kill", c(pid, signal))
}

find_script <- function(name) {
  cmd <- Sys.which(name)
  if (cmd == "") {
    tmp <- tempfile()
    install_scripts(tmp)
    cmd <- file.path(tmp, name)
  }
  cmd
}

hostname <- function() {
  Sys.info()[["nodename"]]
}
process_id <- function() {
  Sys.getpid()
}

## Potentially useful for a monitor thing:
## x <- sample(letters[1:4], 40, replace=TRUE)
## cols <- c(a="red", b="blue", c="green", d="purple")
## pretty_blocks(x, cols)
pretty_blocks <- function(x, cols) {
  sq <- vcapply(cols, function(x) crayon::make_style(x)("\u2588"))
  paste(sq[x], collapse="")
}

## Alternatives:
## http://stackoverflow.com/a/2685827
spin_symbols <- function() {
  sym <- c("-", "\\", "|", "/")
  i <- 0L
  n <- length(sym)
  function() {
    sym[[i <<- if (i >= n) 1L else i + 1L]]
  }
}

progress <- function(total, ..., show=TRUE, prefix="") {
  if (show) {
    fmt <- paste0(prefix, "[:bar] :percent :spin")
    pb <- progress::progress_bar$new(fmt, total=total)
    ws <- spin_symbols()
    function(len=1) {
      invisible(pb$tick(len, list(spin=ws())))
    }
  } else {
    function(...) {}
  }
}

## Short-circuit apply; returns the index of the first element of x
## for which cond(x[[i]]) holds true.
scapply <- function(x, cond, no_match=NA_integer_) {
  for (i in seq_along(x)) {
    if (isTRUE(cond(x[[i]]))) {
      return(i)
    }
  }
  no_match
}

invert_names <- function(x) {
  setNames(names(x), x)
}

blank <- function(n) {
  paste(rep_len(" ", n), collapse="")
}

## Possibly could be done faster.
df_to_list <- function(x) {
  keep <- c("names", "class", "row.names")
  at <- attributes(x)
  attributes(x) <- at[intersect(names(at), keep)]
  unname(lapply(split(x, seq_len(nrow(x))), as.list))
}

match_value <- function(arg, choices, name=deparse(substitute(arg))) {
  assert_scalar_character(arg)
  if (!(arg %in% choices)) {
    stop(sprintf("%s must be one of %s",
                 name, paste(dQuote(choices), collapse=", ")))
  }
  arg
}

create_group <- function(group, verbose) {
  if (is.null(group)) {
    group <- ids::adjective_animal()
    if (verbose) {
      message(sprintf("Creating group: '%s'", group))
    }
  }
  group
}

read_file_to_string <- function(filename) {
  readChar(filename, file.info(filename)$size)
}
write_string_to_file <- function(str, filename) {
  writeChar(str, filename, eos=NULL)
}

##' Install rrqueue scripts.  This currently includes a script for
##' starting a worker process.  The script is extremely simple and
##' does not need upgrading when rrqueue is upgraded.
##' @title Install rrqueue scripts
##' @param dest Directory to install scripts to, preferably in the \code{$PATH}
##' @param overwrite Overwrite any existing file?
##' @export
install_scripts <- function(dest, overwrite=TRUE) {
  src <- system.file("scripts", package=.packageName)
  scripts <- dir(src)
  dir.create(dest, FALSE, TRUE)

  Rscript <- file.path(R.home("bin"), "Rscript")
  for (i in scripts) {
    contents <- readLines(file.path(src, i))
    contents[[1]] <- paste0("#!", Rscript)
    j <- file.path(dest, i)
    if (!file.exists(j) || overwrite) {
      writeLines(contents, j)
      Sys.chmod(j, "0755")
    }
  }
}

yaml_load <- function(string) {
  handlers <- list(`bool#yes` = function(x) {
    if (identical(toupper(x), "TRUE")) TRUE else x
  }, `bool#no` = function(x) {
    if (identical(toupper(x), "FALSE")) FALSE else x
  })
  yaml::yaml.load(string, handlers = handlers)
}
yaml_read <- function(filename) {
  yaml_load(paste(readLines(filename), collapse="\n"))
}

docopt_parse <- function(doc, args, clean=TRUE) {
  oo <- options(warnPartialMatchArgs=FALSE)
  if (isTRUE(oo$warnPartialMatchArgs)) {
    on.exit(options(oo))
  }
  opts <- docopt::docopt(doc, args)
  if (clean) {
    opts <- docopt_clean(opts)
  }
  opts
}

docopt_clean <- function(opts) {
  opts <- opts[!(grepl("^-", names(opts)) | grepl("^<.*>$", names(opts)))]
  names(opts) <- gsub("-", "_", names(opts))
  opts
}

with_wd <- function(path, expr) {
  if (path != ".") {
    if (!file.exists(path)) {
      stop(sprintf("Path '%s' does not exist", path))
    }
    if (!is_directory(path)) {
      stop(sprintf("Path '%s' exists, but is not a directory", path))
    }
    owd <- setwd(path)
    on.exit(setwd(owd))
  }
  force(expr)
}

dir_create <- function(paths) {
  invisible(vlapply(unique(paths), dir.create, FALSE, TRUE))
}

named_list <- function(names) {
  setNames(vector("list", length(names)), names)
}
named_logical <- function(names) {
  setNames(logical(length(names)), names)
}

time_checker <- function(timeout) {
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")
  function() {
    Sys.time() - t0 > timeout
  }
}
