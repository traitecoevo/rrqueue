## Will be prone to false positives but worth a shot
has_namespace <- function(str) {
  grepl("::", str, fixed=TRUE)
}

split_namespace <- function(str) {
  res <- strsplit(str, "::", fixed=TRUE)[[1]]
  if (length(res) != 2L) {
    stop("Not a namespace-qualified variable")
  }
  res
}

exists_function_here <- function(name, envir) {
  exists(name, envir, mode="function", inherits=FALSE)
}
exists_function_ns <- function(name, ns) {
  if (ns %in% .packages()) {
    exists_function_here(name, getNamespace(ns))
  } else {
    FALSE
  }
}

## This is going to search back and find the location of a function by
## descending through environments recursively.
find_function_name <- function(name, envir) {
  if (identical(envir, emptyenv())) {
    stop("Did not find function")
  }
  if (exists_function_here(name, envir)) {
    envir
  } else {
    find_function_name(name, parent.env(envir))
  }
}

find_function_value <- function(fun, envir) {
  if (identical(envir, emptyenv())) {
    stop("Did not find function")
  }
  name <- find_function_in_envir(fun, envir)
  if (!is.null(name)) {
    list(name=name, envir=envir)
  } else {
    find_function_value(fun, parent.env(envir))
  }
}

## Determine the name of a function, given it's value and an
## environment to find it in.
find_function_in_envir <- function(fun, envir) {
  pos <- ls(envir)
  i <- scapply(pos, function(x) identical(fun, envir[[x]]), NULL)
  if (is.null(i)) i else pos[[i]]
}

## TODO: consider `<global>::` and `<local>::` as special names?
## NOTE: This differs from match_fun_symbol because it allows skipping
## up the search path to identify functions in specific parts of the
## search path.  If a namespace-qualified value is given, we can
## ignore envir entirely.
match_fun_name <- function(str, envir) {
  if (has_namespace(str)) {
    ret  <- split_namespace(str)
    if (!exists_function_ns(ret[[2]], ret[[1]])) {
      stop("Did not find function in loaded namespace")
    }
    ret
  } else {
    name <- str
    fun_envir <- find_function_name(name, envir)
    match_fun_sanitise(name, fun_envir)
  }
}

match_fun_symbol <- function(sym, envir) {
  name <- as.character(sym)
  match_fun_name(name, envir)
}

## This one is much harder and might take a while.
##
## TODO: Don't deal here with the case that the function is in
## anything other than the environment that it's enclosure points at;
## that's going to skip memoized functions, etc.  It also is going to
## miss anonymous functions for now.  But start with this bit I think.
##
## TODO: This is going to miss things like extra attributes added to a
## function, but that's going in the category of "users making things
## difficult".
match_fun_value <- function(fun, envir) {
  res <- find_function_value(fun, envir)
  match_fun_sanitise(res$name, res$envir)
}

## TODO: might be worth also passing in 'envir' as the starting
## environment; then we can determine if we're looking at:
##   namespace
##   global env
##   given env
##   other env
## TODO: Might also return the environment here as a named list so
## that we can do some further faffing?
match_fun_sanitise <- function(name, fun_envir) {
  ns <- environmentName(fun_envir)
  ret <- c(sub("^package:", "", ns), name)
  if (ns == "") {
    attr(ret, "envir") <- fun_envir
  }
  ret
}

## TODO: throughout here we'll need to have the functions loaded,
## which is not ideal.
##
## TODO: separate out the NSE from here, like this:
## match_fun <- function(fun, envir) {
##   fun_sub <- substitute(fun)
##   if (is.name(fun_sub)) {
##     match_fun_symbol(fun_sub, envir)
##   } else {
##     match_fun_(fun, envir)
##   }
## }
match_fun <- function(fun, envir) {
  if (is.character(fun)) {
    match_fun_name(fun, envir)
  } else if (is.function(fun)) {
    match_fun_value(fun, envir)
  } else {
    stop("Invalid input")
  }
}
