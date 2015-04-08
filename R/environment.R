create_environment <- function(packages, sources) {
  for (p in packages) {
    library(p, character.only=TRUE, quietly=TRUE)
  }
  catch_source <- function(e) {
    stop(sprintf("while sourcing %s:\n%s", f, e$message),
         call.=FALSE)
  }
  env <- new.env(parent=.GlobalEnv)
  for (f in sources) {
    tryCatch(sys.source(f, env, chdir=TRUE, keep.source=FALSE),
             error=catch_source)
  }
  env
}
