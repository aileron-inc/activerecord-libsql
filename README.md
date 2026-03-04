# activerecord-libsql

ActiveRecord adapter for [Turso](https://turso.tech) (libSQL) database.

Turso に対して ActiveRecord モデルの読み書きができる Ruby gem だよ。
Rust 拡張（[magnus](https://github.com/matsadler/magnus) + [libsql](https://github.com/tursodatabase/libsql)）を使って、
ネイティブに libSQL リモートプロトコルで接続するよ。

## 必要な環境

- Ruby >= 3.1
- Rust >= 1.70（`rustup` でインストール）
- ActiveRecord >= 7.0

## インストール

```ruby
# Gemfile
gem "activerecord-libsql"
```

```bash
bundle install
```

## セットアップ

### 1. Rust のインストール

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### 2. gem のビルド

```bash
bundle exec rake compile
```

### 3. database.yml の設定

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

> **Note**: `url:` キーではなく `database:` キーを使うこと。`url:` を使うと ActiveRecord が URL スキームからアダプターを自動解決しようとして失敗する。

## 使い方

```ruby
require "activerecord-libsql"

# 直接接続する場合
ActiveRecord::Base.establish_connection(
  adapter:  "turso",
  database: "libsql://your-db.turso.io",
  token:    "your-auth-token"
)

# モデル定義
class User < ActiveRecord::Base
end

# CRUD
User.create!(name: "Alice", email: "alice@example.com")
User.where(name: "Alice").first
User.find(1).update!(email: "new@example.com")
User.find(1).destroy
```

## アーキテクチャ

```
Rails Model (ActiveRecord)
  ↓ Arel → SQL 文字列
LibsqlAdapter (lib/active_record/connection_adapters/libsql_adapter.rb)
  ↓ exec_query / exec_update
TursoLibsql::Connection (Rust 拡張)
  ↓ libsql::Connection (async → block_on)
Turso クラウド (libSQL リモートプロトコル)
```

## スレッド安全性

`libsql::Connection` は `Send + Sync` を実装しているためスレッドセーフだよ。
ActiveRecord の ConnectionPool はスレッドごとに独立した Adapter インスタンスを払い出すため、
`@raw_connection` の競合は発生しない。

## 現在の対応状況

| 機能 | 状態 |
|------|------|
| SELECT | ✅ |
| INSERT | ✅ |
| UPDATE | ✅ |
| DELETE | ✅ |
| トランザクション | ✅ |
| マイグレーション（基本） | ✅ |
| プリペアドステートメント | ✅ |
| BLOB | ✅ |
| Embedded Replica | 🔜 |

## ライセンス

MIT
