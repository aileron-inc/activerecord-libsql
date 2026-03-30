# frozen_string_literal: true

require_relative 'connection'

module TursoLibsql
  # Database ラッパー
  # リモート接続と Embedded Replica の両方をサポート
  class Database
    # リモート接続用 Database を作成
    def self.new_remote(url, token)
      new(mode: :remote, url: url, token: token)
    end

    # Embedded Replica 用 Database を作成
    def self.new_remote_replica(path, url, token, sync_interval_secs = 0)
      new(mode: :replica, path: path, url: url, token: token, sync_interval: sync_interval_secs)
    end

    # Offline write 用 Database を作成
    def self.new_synced(path, url, token, sync_interval_secs = 0)
      new(mode: :offline, path: path, url: url, token: token, sync_interval: sync_interval_secs)
    end

    def initialize(mode:, url: nil, token: nil, path: nil, sync_interval: 0)
      @mode = mode
      @url = url
      @token = token || ''
      @path = path
      @sync_interval = sync_interval
    end

    # この Database から Connection を取得して返す
    def connect
      case @mode
      when :remote
        Connection.new(@url, @token)
      when :replica, :offline
        # ローカルファイルを開く（なければ作成される）
        LocalConnection.new(@path, @url, @token, @mode)
      end
    end

    # リモートから最新フレームを手動で同期する
    def sync
      case @mode
      when :remote
        # remote モードでは no-op
        nil
      when :replica, :offline
        replica_sync(@path, @url, @token, @mode == :offline)
      end
    end

    private

    def replica_sync(path, url, token, offline)
      remote_conn = Connection.new(url, token)

      # ローカル DB を開く
      require 'sqlite3'
      local = SQLite3::Database.new(path)
      local.results_as_hash = true

      # remote からテーブル一覧を取得
      tables = remote_conn.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
      ).map { |r| r['name'] }

      tables.each do |table|
        # remote からスキーマを取得
        schema_rows = remote_conn.query(
          "SELECT sql FROM sqlite_master WHERE type='table' AND name='#{table.gsub("'", "''")}'"
        )
        next if schema_rows.empty?

        schema = schema_rows.first['sql']
        # CREATE TABLE "foo" (...) → CREATE TABLE IF NOT EXISTS "foo" (...)
        create_sql = schema.sub(/\ACREATE TABLE\b/i, 'CREATE TABLE IF NOT EXISTS')
        local.execute(create_sql)

        # pull: remote → local（offline モードも pull してからローカル write を優先）
        remote_rows = remote_conn.query("SELECT * FROM \"#{table.gsub('"', '""')}\"")
        remote_rows.each do |row|
          cols = row.keys.map { |c| "\"#{c.gsub('"', '""')}\"" }.join(', ')
          vals = row.values.map { |v| v.nil? ? 'NULL' : "'#{v.to_s.gsub("'", "''")}'" }.join(', ')
          local.execute("INSERT OR REPLACE INTO \"#{table.gsub('"', '""')}\" (#{cols}) VALUES (#{vals})")
        end

        next unless offline

        # push: local → remote（offline モードのみ）
        # results_as_hash = true なので Hash の配列が返る
        local_rows = local.execute("SELECT * FROM \"#{table.gsub('"', '""')}\"")
        next if local_rows.empty?

        local_rows.each do |row|
          cols = row.keys.map { |c| "\"#{c.gsub('"', '""')}\"" }.join(', ')
          vals = row.values.map { |v| v.nil? ? 'NULL' : "'#{v.to_s.gsub("'", "''")}'" }.join(', ')
          remote_conn.execute("INSERT OR REPLACE INTO \"#{table.gsub('"', '""')}\" (#{cols}) VALUES (#{vals})")
        end
      end

      local.close
    end
  end

  # ローカル SQLite 接続（Embedded Replica 用）
  # sqlite3 gem を使用
  # sqlite3 gem 2.x は fork 後に接続を自動クローズする（ForkSafety）。
  # AR の discard! が @raw_connection を nil にするので、子プロセスでは
  # reconnect が走って新しい接続が確立される。
  class LocalConnection
    # Solid Queue など複数プロセスが同時に書き込む場合のロック待機時間（ミリ秒）。
    # デフォルトの 0ms だと即 SQLite3::BusyException になる。
    BUSY_TIMEOUT_MS = 5000

    def initialize(path, remote_url, token, mode)
      require 'sqlite3'
      @path = path
      @remote_url = remote_url
      @token = token
      @mode = mode
      @db = SQLite3::Database.new(path)
      @db.results_as_hash = true
      # WAL モード: 読み取りと書き込みを並行できる。
      # デフォルトの DELETE ジャーナルモードは同時書き込みで database is locked になる。
      # Solid Queue のように複数 fork が同じファイルに書く場合に必須。
      @db.execute('PRAGMA journal_mode=WAL')
      # ロック競合時に即エラーにならず、指定ミリ秒待ってリトライする。
      @db.execute("PRAGMA busy_timeout=#{BUSY_TIMEOUT_MS}")
      @last_insert_rowid = 0
      @last_affected_rows = 0
    end

    def execute(sql)
      @db.execute(sql)
      @last_affected_rows = @db.changes
      @last_insert_rowid = @db.last_insert_row_id
      @last_affected_rows
    end

    def query(sql)
      @db.execute(sql)
    end

    def execute_with_params(sql, params)
      @db.execute(sql, params)
      @last_affected_rows = @db.changes
      @last_insert_rowid = @db.last_insert_row_id
      @last_affected_rows
    end

    def begin_transaction
      @db.execute('BEGIN')
    end

    def commit_transaction
      @db.execute('COMMIT')
    rescue SQLite3::Exception => e
      # fork 後に接続が強制クローズされた場合など、トランザクションが
      # 既に存在しない状態での COMMIT は無視する
      raise unless e.message.include?('no transaction is active')
    end

    def rollback_transaction
      @db.execute('ROLLBACK')
    rescue SQLite3::Exception => e
      # fork 後に接続が強制クローズされた場合など、トランザクションが
      # 既に存在しない状態での ROLLBACK は無視する
      raise unless e.message.include?('no transaction is active')
    end

    attr_reader :last_insert_rowid
  end
end
