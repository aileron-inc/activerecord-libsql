# activerecord-libsql

ActiveRecord adapter for [Turso](https://turso.tech) (libSQL) database.

Connects Rails / ActiveRecord models to Turso Cloud via the **Hrana v2 HTTP protocol**,
implemented in pure Ruby using `Net::HTTP` — no native extension, no Rust toolchain required.

## Requirements

- Ruby >= 3.1
- ActiveRecord >= 7.0

## Installation

```ruby
# Gemfile
gem "activerecord-libsql"
```

```bash
bundle install
```

No compile step needed. The gem is pure Ruby.

## Configuration

### database.yml

```yaml
default: &default
  adapter: turso
  database: <%= ENV["TURSO_DATABASE_URL"] %>   # libsql://xxx.turso.io
  token: <%= ENV["TURSO_AUTH_TOKEN"] %>

development:
  <<: *default

production:
  <<: *default
```

> **Note**: Use the `database:` key, not `url:`. ActiveRecord tries to resolve the adapter
> from the URL scheme when `url:` is used, which causes a lookup failure.

### Direct connection

```ruby
require "activerecord-libsql"

ActiveRecord::Base.establish_connection(
  adapter:  "turso",
  database: "libsql://your-db.turso.io",
  token:    "your-auth-token"
)
```

## Usage

```ruby
class User < ActiveRecord::Base
end

# Create
User.create!(name: "Alice", email: "alice@example.com")

# Read
User.where(name: "Alice").first
User.find(1)
User.order(:name).limit(10)

# Update
User.find(1).update!(email: "new@example.com")

# Delete
User.find(1).destroy
```

## Embedded Replicas

Embedded Replicas keep a local SQLite copy of your Turso database on disk, synced from the
remote. Reads are served locally (sub-millisecond), writes go to the remote.

### Configuration

```yaml
# database.yml
production:
  adapter: turso
  database: <%= ENV["TURSO_DATABASE_URL"] %>   # libsql://xxx.turso.io
  token: <%= ENV["TURSO_AUTH_TOKEN"] %>
  replica_path: /var/data/myapp.db             # local replica file path
  sync_interval: 60                            # background sync every 60 seconds (0 = manual only)
```

Or via `establish_connection`:

```ruby
ActiveRecord::Base.establish_connection(
  adapter:       "turso",
  database:      "libsql://your-db.turso.io",
  token:         "your-auth-token",
  replica_path:  "/var/data/myapp.db",
  sync_interval: 60
)
```

### Manual sync

```ruby
# Trigger a sync from the remote at any time
ActiveRecord::Base.connection.sync
```

### Notes

- `replica_path` must point to a writable path. The file is created automatically on first connect.
- `sync_interval` is in seconds. Set to `0` or omit to use manual sync only.
- **Multi-process (Puma / Solid Queue)**: Each worker process gets its own SQLite connection.
  `sqlite3` gem 2.x handles fork safety automatically — connections are closed after `fork()`
  and reopened in the child process. Do not share the same `replica_path` across multiple
  Puma workers; use a unique path per worker (e.g. `/var/data/myapp-worker-#{worker_id}.db`).

## Solid Queue

This adapter is compatible with [Solid Queue](https://github.com/rails/solid_queue).

### Known behaviour

- **`FOR UPDATE SKIP LOCKED`**: Solid Queue uses this clause by default. libSQL and SQLite
  do not support it, so the adapter strips it automatically before sending SQL to the backend.
  SQLite serializes all writes, so row-level locking is not needed.
- **Fork safety**: Solid Queue forks worker processes. The adapter handles this correctly —
  `sqlite3` gem 2.x closes connections after `fork()`, and ActiveRecord's `discard!` /
  `reconnect` flow re-establishes them in the child process.

### Example config

```yaml
# config/queue.yml
default: &default
  dispatchers:
    - polling_interval: 1
      batch_size: 500
  workers:
    - queues: "*"
      threads: 3
      polling_interval: 0.1
```

## Schema Management

`turso:schema:apply` and `turso:schema:diff` use [sqldef](https://github.com/sqldef/sqldef)
(`sqlite3def`) to manage your Turso schema declaratively — no migration files, no version
tracking. You define the desired schema in a `.sql` file and the task computes and applies
only the diff.

### Prerequisites

```bash
# macOS
brew install sqldef/sqldef/sqlite3def

# Other platforms: https://github.com/sqldef/sqldef/releases
```

`replica_path` must be configured in `database.yml` (the tasks use the local replica to
compute the diff without touching the remote directly).

### turso:schema:apply

Applies the diff between your desired schema and the current remote schema.

```bash
rake turso:schema:apply[db/schema.sql]
```

### turso:schema:diff

Shows what would be applied without making any changes (dry-run).

```bash
rake turso:schema:diff[db/schema.sql]
```

### schema.sql format

Plain SQL `CREATE TABLE` statements. sqldef handles `ALTER TABLE` / `CREATE INDEX` / `DROP`
automatically based on the diff.

```sql
CREATE TABLE users (
  id   TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL
);
```

## Architecture

```
Rails Model (ActiveRecord)
  ↓  Arel → SQL string
LibsqlAdapter  (lib/active_record/connection_adapters/libsql_adapter.rb)
  ↓  perform_query  (strips FOR UPDATE, expands bind params)
  ↓
  ├─ Remote mode ──→ TursoLibsql::Connection  (Hrana v2 HTTP via Net::HTTP)
  │                       ↓
  │                  Turso Cloud  (HTTPS)
  │
  └─ Embedded Replica / Offline mode
         ↓
    TursoLibsql::LocalConnection  (sqlite3 gem)
         ↓
    Local SQLite file  ←─ sync ─→  Turso Cloud
```

### Why pure Ruby?

The original implementation used a Rust native extension (`tokio` + `rustls`). On macOS,
all Rust TLS libraries are unsafe after `fork()` — they cause SEGV or deadlocks in
multi-process servers (Puma, Unicorn, Solid Queue). Ruby's `Net::HTTP` has no such
restriction and is fully fork-safe.

## Thread Safety

ActiveRecord's `ConnectionPool` issues a separate `Adapter` instance per thread, so
`@raw_connection` is never shared across threads. `Net::HTTP` opens a new TCP connection
per request, which is safe for concurrent use.

## Performance

Benchmarked against a **Turso cloud database** (remote, over HTTPS) from a MacBook on a
home network. All numbers include full round-trip network latency.

| Operation | ops/sec | avg latency |
|-----------|--------:|------------:|
| INSERT single row | 9.9 | 101.5 ms |
| SELECT all (100 rows) | 29.1 | 34.3 ms |
| SELECT WHERE | 35.9 | 27.9 ms |
| SELECT find by id | 16.2 | 61.9 ms |
| UPDATE single row | 6.4 | 156.0 ms |
| DELETE single row | 6.9 | 145.2 ms |
| Transaction (10 inserts) | 1.9 | 539.0 ms |

> **Environment**: Ruby 3.4.8 · ActiveRecord 8.1.2 · Turso cloud (remote) · macOS arm64
> Run `bundle exec ruby bench/benchmark.rb` to reproduce.

Latency is dominated by network round-trips to the Turso cloud endpoint. For lower latency,
use [Embedded Replicas](#embedded-replicas) — reads are served from a local SQLite file with
sub-millisecond latency.

## Feature Support

| Feature | Status |
|---------|--------|
| SELECT / INSERT / UPDATE / DELETE | ✅ |
| Transactions | ✅ |
| Migrations (basic DDL) | ✅ |
| Schema management (sqldef) | ✅ |
| Bind parameters | ✅ |
| NOT NULL / UNIQUE / FK constraint → AR exceptions | ✅ |
| Embedded Replica (local reads) | ✅ |
| Offline write mode | ✅ |
| Solid Queue compatibility | ✅ |
| Fork safety (Puma / Solid Queue) | ✅ |
| Prepared statements (server-side) | ❌ libSQL HTTP does not support them |
| EXPLAIN | ❌ |
| Savepoints | ❌ |

## Testing

```bash
# Unit tests only (no credentials needed)
bundle exec rake spec

# Integration tests (requires TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)
bundle exec rake spec:integration

# All tests
bundle exec rake spec:all
```

Set `SKIP_INTEGRATION_TESTS=1` to skip integration tests in CI environments without Turso
credentials.

## License

MIT
