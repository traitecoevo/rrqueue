create_environment <- function(packages, sources) {
  for (p in packages) {
    library(p, character.only=TRUE, quietly=TRUE)
  }
  env <- new.env(parent=.GlobalEnv)
  for (f in sources) {
    tryCatch(sys.source(f, env, chdir=TRUE, keep.source=FALSE),
             error=catch_source)
  }
  env
}

## An alternative here is to trace some of the connection code, but
## that might be harder.  The issue here is that we're going to be
## missing things like csv files, configuration files, etc, that might
## determine the state of the system.  I don't think that there's much
## we can do about that though.  For cases where we're running locally
## it'll be fine.
create_environment2 <- function(packages, sources, env) {
  for (p in packages) {
    library(p, character.only=TRUE, quietly=TRUE)
  }

  source_files <- character(0)
  for (file in sources) {
    source_files <- c(
      source_files,
      tryCatch(sys_source(file, env, chdir=TRUE, keep.source=FALSE),
               error=catch_source))
  }

  source_files
}

catch_source <- function(e) {
  stop(sprintf("while sourcing %s:\n%s", f, e$message),
       call.=FALSE)
}
