# Changelog

All notable changes to this project will be documented in this file.

## [0.1.5] - 2026-03-13

### Fixed

- **`FOR UPDATE SKIP LOCKED` syntax error with Solid Queue** (#12)
  - Solid Queue uses `SELECT ... FOR UPDATE SKIP LOCKED` by default (`use_skip_locked: true`).
    libSQL and SQLite do not support this clause, causing a parse error at the backend.
  - `LibsqlAdapter#perform_query` now strips `FOR UPDATE` / `FOR UPDATE SKIP LOCKED` / `FOR SHARE`
    before sending SQL to either the Hrana HTTP API or the local SQLite file.
  - SQLite serializes all writes, so row-level locking is semantically unnecessary.
  - Added a regression test that reproduces the exact query Solid Queue's Dispatcher sends.

## [0.1.4] - 2026-03-13

### Fixed

- **Fork safety for Embedded Replica mode** (#9)
  - `sqlite3` gem 2.x automatically closes SQLite connections after `fork()` (ForkSafety).
    In Solid Queue, the supervisor forks worker processes. If a transaction was open at fork
    time, the child process would receive `cannot commit/rollback - no transaction is active`.
  - `LocalConnection#commit_transaction` and `#rollback_transaction` now rescue
    `SQLite3::Exception` and silently ignore the "no transaction is active" case.
  - ActiveRecord's `discard!` already nils `@raw_connection`, so child processes reconnect
    cleanly on the next query.

### Changed

- Rakefile: removed `rb_sys` / `rake/extensiontask` dependencies (no longer needed after #7).
  `task default` changed from `:compile` to `:spec`.
- `rake release` now uses `jj bookmark track` before `jj git push` to avoid the
  "Refusing to create new remote bookmark" error for new version tags.

## [0.1.3] - 2026-03-13

### Changed

- **Replaced Rust native extension with pure Ruby implementation** (#7)
  - The original Rust extension used `tokio` + `rustls` (and later `curl` + OpenSSL) for HTTP
    transport. All Rust TLS implementations are unsafe after `fork()` on macOS, causing SEGV
    or deadlocks in Puma / Unicorn / Solid Queue multi-process environments.
  - New implementation uses Ruby's built-in `Net::HTTP` (fork-safe, no native dependencies).
  - Hrana v2 HTTP protocol is implemented directly in `TursoLibsql::Connection`.
  - `TursoLibsql::Database` provides a factory for remote, Embedded Replica, and offline modes.
  - `TursoLibsql.reinitialize_runtime!` is now a no-op (kept for API compatibility).
  - Added `fork_spec.rb` with 7 integration examples verifying fork safety.

### Added

- `sqlite3 >= 1.4` runtime dependency for Embedded Replica / offline write mode.

### Removed

- Rust extension (`ext/turso_libsql/`), `rb_sys`, and `rake-compiler` dependencies.
  No `bundle exec rake compile` step is needed anymore.

### Fixed

- `replica_sync`: SQL generation bug — `CREATE TABLE "foo" (...)` was being mangled.
  Fixed by using `schema.sub(/\ACREATE TABLE\b/i, 'CREATE TABLE IF NOT EXISTS')`.
- Offline push: `results_as_hash = true` returns Hash rows; use `row.keys` / `row.values`.

## [0.1.2] - 2026-03-12

### Fixed

- `ActiveRecord::Result.empty` compatibility for `affected_rows` (#5).
  `Result.empty` is a frozen singleton in AR 8; calling `.rows` on it is safe but
  the adapter was incorrectly reading `affected_row_count` from it.
- Added Solid Queue integration tests (10 examples) covering `INSERT` into
  `solid_queue_processes`, `solid_queue_jobs`, and related tables.

## [0.1.1] - 2026-03-12

### Fixed

- `Column.new` signature compatibility across ActiveRecord versions (#4):
  - AR <= 8.0: `Column.new(name, default, sql_type_metadata, null)`
  - AR >= 8.1: `Column.new(name, cast_type, default, sql_type_metadata, null)`
  - Resolved at load time via `COLUMN_BUILDER` lambda to avoid per-call version checks.

## [0.1.0] - 2026-03-10

### Added

- Initial release: ActiveRecord adapter for Turso (libSQL) using a Rust native extension.
- Remote connection mode via libSQL remote protocol.
- Embedded Replica mode (`replica_path:`) with optional background sync (`sync_interval:`).
- Offline write mode (`offline: true`) — writes locally, syncs to remote on `#sync`.
- `rake turso:schema:apply` and `rake turso:schema:diff` via sqldef (#2).
- `rake build`, `rake install`, `rake release` tasks (#3).
- Unit and integration test suite — 57 examples (#1).
