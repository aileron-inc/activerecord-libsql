# activerecord-libsql

ActiveRecord adapter for [Turso](https://turso.tech) (libSQL) database.

Connects Rails/ActiveRecord models to Turso via a native Rust extension ([magnus](https://github.com/matsadler/magnus) + [libsql](https://github.com/tursodatabase/libsql)), using the libSQL remote protocol directly — no HTTP client wrapper required.

## Requirements

- Ruby >= 3.1
- Rust >= 1.70 (install via [rustup](https://rustup.rs))
- ActiveRecord >= 7.0

## Installation

```ruby
# Gemfile
gem "activerecord-libsql"
```

```bash
bundle install
bundle exec rake compile
```

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

> **Note**: Use the `database:` key, not `url:`. ActiveRecord tries to resolve the adapter from the URL scheme when `url:` is used, which causes a lookup failure.

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

Embedded Replicas keep a local SQLite copy of your Turso database on disk, synced from the remote. Reads are served locally (sub-millisecond), writes go to the remote.

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

- `replica_path` must point to a clean (empty) file or a previously synced replica. Using an existing SQLite file from another source will cause an error.
- `sync_interval` is in seconds. Set to `0` or omit to use manual sync only.
- **Multi-process caution**: Do not share the same `replica_path` across multiple Puma workers. Each worker should use a unique path (e.g. `/var/data/myapp-worker-#{worker_id}.db`).
- The background sync task runs as long as the `Database` object is alive. The adapter holds the `Database` for the lifetime of the connection.

## Architecture

```
Rails Model (ActiveRecord)
  ↓  Arel → SQL string
LibsqlAdapter  (lib/active_record/connection_adapters/libsql_adapter.rb)
  ↓  perform_query / exec_update
TursoLibsql::Database + Connection  (Rust native extension)
  ↓  libsql::Database / Connection  (async Tokio runtime → block_on)

Remote mode:   Turso Cloud  (libSQL remote protocol over HTTPS)
Replica mode:  Local SQLite file ←sync→ Turso Cloud
```

## Thread Safety

`libsql::Connection` implements `Send + Sync`, making it thread-safe. ActiveRecord's `ConnectionPool` issues a separate `Adapter` instance per thread, so `@raw_connection` is never shared across threads.

## Performance

Benchmarked against a **Turso cloud database** (remote, over HTTPS) from a MacBook on a home network. All numbers include full round-trip network latency.

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

Latency is dominated by network round-trips to the Turso cloud endpoint. For lower latency, use [Embedded Replicas](#embedded-replicas) — reads are served from a local SQLite file with sub-millisecond latency.

## Feature Support

| Feature | Status |
|---------|--------|
| SELECT | ✅ |
| INSERT | ✅ |
| UPDATE | ✅ |
| DELETE | ✅ |
| Transactions | ✅ |
| Migrations (basic) | ✅ |
| Prepared statements | ✅ |
| BLOB | ✅ |
| NOT NULL / UNIQUE constraint errors → AR exceptions | ✅ |
| Embedded Replica | ✅ |

## Testing

```bash
# Unit tests only (no credentials needed)
bundle exec rake spec

# Integration tests (requires TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)
bundle exec rake spec:integration

# All tests
bundle exec rake spec:all
```

Set `SKIP_INTEGRATION_TESTS=1` to skip integration tests in CI environments without Turso credentials.

## License

MIT
