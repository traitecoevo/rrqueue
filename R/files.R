files_pack <- function(cache, ..., files=c(...)) {
  ## For now, assume text files only.
  pack1 <- function(filename) {
    ## NOTE: this duplicates the content addressable storage in storr,
    ## I think.  But the label is not going to change and we want to
    ## manage the mapping in rrqueue.
    contents <- read_file_to_string(filename)
    hash <- hash_string(contents)
    cache$set(hash, contents)
    setNames(hash, filename)
  }
  ret <- vcapply(files, pack1)
  class(ret) <- "files_pack"
  ret
}

files_unpack <- function(cache, pack, path=tempfile()) {
  unpack1 <- function(x) {
    filename <- file.path(path, x)
    dir.create(dirname(filename), FALSE, TRUE)
    contents <- cache$get(pack[[x]])
    write_string_to_file(contents, filename)
  }
  lapply(names(pack), unpack1)
  invisible(path)
}
