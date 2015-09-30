##' Load environment variables from a yaml file.  This is a hack for a
##' project.  It may change and may move package.  \code{callr} would
##' be a better fit probably, but \code{callr} doesn't pull in
##' \code{yaml} yet so I don't know that it's a good fit.
##'
##' The yaml file must be sets of key/value pairs of simple data
##' types.  Something like:
##'
##' \preformatted{
##' REDIS_HOST: localhost
##' }
##'
##' Alternatively, for use with section, add an extra layer of nesting:
##'
##' \preformatted{
##' local:
##'   REDIS_HOST: localhost
##' remote:
##'   REDIS_HOST: redis.marathon.mesos
##' }
##'
##' @title Load environment variables from a yaml file
##' @param filename Name of the file
##' @param section An optional section of the file to load
##' @export
##'
yaml_env <- function(filename, section=NULL) {
  data <- yaml_read(filename)
  if (!is.null(section)) {
    if (section %in% names(data)) {
      data <- data[[section]]
    } else {
      stop(sprintf("section %d not found", section))
    }
  }
  check <- function(x) {
    is.atomic(x) && length(x) == 1L
  }
  ok <- vlapply(data, check)
  if (!all(ok)) {
    stop("Unexpected type for ", paste(names(data)[!ok], collapse=", "))
  }
  if (length(data) > 0L) {
    do.call("Sys.setenv", data)
  }
  invisible(names(data))
}
