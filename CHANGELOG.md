# Changelog

All notable changes to this project will be documented in this file.

## [0.1.7] - 2026-03-27

### Fixed

- **`NotImplementedError` on `insert_all!` / `upsert_all` with Solid Queue** (#17)
  - Solid Queue's `ClaimedExecution.claiming` calls `insert_all!`, which internally calls
    `self.class.quote_column_name` (a class-level method).
  - `AbstractAdapter::ClassMethods#quote_column_name` raises `NotImplementedError` by design.
    `LibsqlAdapter` only defined the instance-level method, leaving the class-level one unimplemented.
  - Added `class << self` block with `quote_column_name` / `quote_table_name` (cached via `Concurrent::Map`).
  - Removed redundant instance-level `quote_column_name` / `quote_table_name` — AR 8's `Quoting` module
    delegates instance calls to `self.class.quote_column_name`, so only the class-level definition is needed.
  - Added `quote_string` override: SQLite / libSQL do not treat backslash as an escape character,
    so the default `AbstractAdapter` implementation (which doubles backslashes) would corrupt values
    containing `\`. Now only single-quotes are escaped, matching `SQLite3Adapter`.
  - Implemented `build_insert_sql` with `ON CONFLICT ... DO NOTHING / DO UPDATE SET` syntax
    (same as `SQLite3Adapter`).
  - Declared `supports_insert_on_duplicate_skip?`, `supports_insert_on_duplicate_update?`,
    and `supports_insert_conflict_target?` as `true`.

## [0.1.6] - 2026-03-16

### Fixed

- **`datetime` columns not stored in UTC, breaking `WHERE scheduled_at <= ?` comparisons**
  - `NATIVE_DATABASE_TYPES` mapped `datetime`/`timestamp` to `'TEXT'`, causing `PRAGMA table_info`
    to return `TEXT` for those columns. ActiveRecord's type map then resolved them to `Type::Text`,
    which serializes `Time` objects via `to_s` without UTC normalization.
  - Changed `datetime` → `'datetime'`, `timestamp` → `'datetime'`, `time` → `'time'`,
    `date` → `'date'` so that `PRAGMA table_info` returns the correct type name and AR maps
    them to `Type::DateTime` / `Type::Time` / `Type::Date`.
  - `Type::DateTime` serializes `Time` objects to UTC strings (e.g. `"2026-03-16 04:18:14"`),
    making string-based `<=` / `>=` comparisons consistent.
  - This fixes Solid Queue's Dispatcher, which queries `WHERE scheduled_at <= ?` to find
    due jobs — the query was always returning empty results when the server timezone was non-UTC.
  - Added `initialize_type_map` override to explicitly register `datetime`/`timestamp` →
    `Type::DateTime` as a safety net for existing databases with `TEXT`-typed datetime columns.

- **`cannot commit - no transaction is active` in Hrana HTTP client after fork**
  - `TursoLibsql::Connection#commit_transaction` now rescues `RuntimeError` containing
    `"no transaction is active"` or `"cannot commit"`, matching the existing behavior of
    `LocalConnection` (SQLite3 backend) added in v0.1.4.
  - Occurs when `ActiveSupport::ForkTracker` triggers `PoolConfig.discard_pools!` after fork,
    and AR attempts to commit a transaction on the discarded connection before reconnecting.

### Added

- **Solid Queue fork simulation integration tests** (`spec/integration/solid_queue_fork_spec.rb`)
  - 5 examples reproducing the actual Solid Queue supervisor → worker fork flow.
  - Verifies that child processes can `INSERT`, call `SolidQueue::Process.create!`,
    run `FOR UPDATE SKIP LOCKED` queries, and that multiple concurrent workers all succeed.
  - Verifies that the parent process continues to work after child forks.

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
