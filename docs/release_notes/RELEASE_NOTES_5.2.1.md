# uni-db 5.2.1

Changes since [5.2.0](RELEASE_NOTES_5.2.0.md).

5.2.1 is a patch release: it makes uni-db work on Windows again. It has no
API or behaviour changes on Linux or macOS.

## Correctness fixes

- **On Windows, the first write to any store failed** with `Access is
  denied. (os error 5)`, including a write to `Uni::in_memory()`, which is
  backed by a temporary directory (#338). Each newly written WAL segment,
  snapshot manifest and catalog file was reopened read-only before
  `sync_all`. On Windows that call is `FlushFileBuffers`, which needs write
  access on the handle. The file is now opened for writing. POSIX `fsync`
  accepts either kind of handle, so nothing changes on Unix. The bug was
  present in 5.1.0 and 5.2.0. A unit test now syncs a freshly written file.
