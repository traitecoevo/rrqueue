#!/usr/bin/env Rscript

## Dirty hack to compile docs in the absence of proper Roxygen R6 support.
library(rrqueue)

add_usage <- function(dat, generator) {
  capture_usage <- function(name) {
    cl <- as.call(c(pairlist(as.symbol(name)),
                    formals(generator$public_methods[[name]])))
    paste(capture.output(print(cl)), collapse="\n")
  }

  for (name in names(dat)) {
    ## TODO: check for exactly all parameters and the order.
    dat[[name]]$method_name <- name
    dat[[name]]$usage <- capture_usage(name)
    dat[[name]]$order <- names(formals(generator$public_methods[[name]]))
  }
  dat
}

indent <- function(str, n, pad=NULL) {
  if (is.null(pad)) {
    pad <- paste(rep(" ", n), collapse="")
  }
  p <- function(s) {
    paste(paste0(pad, s), collapse="\n")
  }
  vapply(strsplit(str, "\n"), p, character(1))
}

format_params <- function(xp) {
  fmt1 <- "\\describe{\n%s\n}"
  fmt2 <- "\\item{\\code{%s}}{\n%s\n}\n"
  pars <- sprintf(fmt2, names(xp), indent(unlist(xp), 2))
  sprintf(fmt1, indent(paste(pars, collapse="\n"), 2))
}

format_method <- function(x) {
  title <- sprintf("\\item{\\code{%s}}{", x$method_name)
  end <- "}"

  body <- sprintf("%s\n\n\\emph{Usage:}\n\\code{%s}",
                  x$short, x$usage)
  if (!is.null(x$params)) {
    body <- paste0(body, "\n\n\\emph{Arguments:}\n", format_params(x$params))
  }
  if (!is.null(x$details)) {
    body <- paste0(body, "\n\n\\emph{Details:}\n", x$details)
  }
  if (!is.null(x$value)) {
    body <- paste0(body, "\n\n\\emph{Value}:\n", x$value)
  }
  paste(title, indent(body, 2), end, sep="\n")
}

strip_trailing_whitespace <- function(x) {
  gsub("[ t]+(\n|$)", "\\1", x)
}

format_class <- function(x) {
  ret <- vapply(x, format_method, character(1))
  ret <- sprintf("@section Methods:\n\n\\describe{\n%s\n}",
                 paste(ret, collapse="\n"))
  ret <- indent(ret, pad="##' ")
  strip_trailing_whitespace(ret)
}

process <- function(type) {
  e <- environment(rrqueue::queue)
  generator <- get(sprintf(".R6_%s", type), e)
  dat <- rrqueue:::yaml_read(sprintf("man-roxygen/%s.yml", type))
  ret <- format_class(add_usage(dat, generator))
  writeLines(ret, sprintf("man-roxygen/%s_methods.R", type))
}

types <- function() {
  re <- "\\.yml$"
  sub(re, "", dir("man-roxygen", pattern=re))
}

process_all <- function() {
  for (t in types()) {
    message(paste("Generating:", t))
    process(t)
  }
}

if (!interactive() && identical(commandArgs(TRUE), "process")) {
  process_all()
}
