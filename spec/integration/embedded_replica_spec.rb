# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'

# Embedded Replica / Offline Write 統合テスト
# TURSO_DATABASE_URL と TURSO_AUTH_TOKEN が必要
# CI 環境では SKIP_INTEGRATION_TESTS=1 で全スキップ可能
RSpec.describe 'Embedded Replica', :integration do
  before(:all) do
    skip 'Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run integration tests' \
      unless ENV['TURSO_DATABASE_URL'] && ENV['TURSO_AUTH_TOKEN']
    skip 'Integration tests skipped (SKIP_INTEGRATION_TESTS=1)' \
      if ENV['SKIP_INTEGRATION_TESTS'] == '1'
  end

  let(:url)          { ENV['TURSO_DATABASE_URL'] }
  let(:token)        { ENV['TURSO_AUTH_TOKEN'] }
  let(:tmpdir)       { Dir.mktmpdir('libsql_replica_spec') }
  let(:replica_path) { File.join(tmpdir, 'replica.db') }
  let(:offline_path) { File.join(tmpdir, 'offline.db') }

  after do
    FileUtils.remove_entry(tmpdir) if File.exist?(tmpdir)
  end

  # -----------------------------------------------------------------------
  # TursoLibsql::Database — low-level API
  # -----------------------------------------------------------------------

  describe TursoLibsql::Database do
    describe '.new_remote' do
      it 'creates a remote database without error' do
        expect do
          TursoLibsql::Database.new_remote(url, token)
        end.not_to raise_error
      end

      it 'returns a connection that can execute SELECT 1' do
        db   = TursoLibsql::Database.new_remote(url, token)
        conn = db.connect
        expect(conn.query('SELECT 1 AS n')).to eq([{ 'n' => 1 }])
      end
    end

    describe '.new_remote_replica' do
      it 'creates a database without error' do
        expect do
          TursoLibsql::Database.new_remote_replica(replica_path, url, token, 0)
        end.not_to raise_error
      end

      it 'creates a local replica file after connecting' do
        db = TursoLibsql::Database.new_remote_replica(replica_path, url, token, 0)
        db.connect
        expect(File.exist?(replica_path)).to be true
      end

      it 'returns a connection that can execute SELECT 1' do
        db   = TursoLibsql::Database.new_remote_replica(replica_path, url, token, 0)
        conn = db.connect
        expect(conn.query('SELECT 1 AS n')).to eq([{ 'n' => 1 }])
      end

      it 'syncs from remote without error' do
        db = TursoLibsql::Database.new_remote_replica(replica_path, url, token, 0)
        db.connect
        expect { db.sync }.not_to raise_error
      end
    end

    describe '.new_synced (offline write mode)' do
      it 'creates a database without error' do
        expect do
          TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        end.not_to raise_error
      end

      it 'creates a local db file after connecting' do
        db = TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        db.connect
        expect(File.exist?(offline_path)).to be true
      end

      it 'can pull from remote (sync)' do
        db = TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        db.connect
        expect { db.sync }.not_to raise_error
      end

      it 'can write locally without remote round-trip' do
        db   = TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        conn = db.connect
        db.sync
        conn.execute('CREATE TABLE IF NOT EXISTS offline_test (id TEXT PRIMARY KEY, val TEXT)')
        # write はローカルだけ（リモートへは飛ばない）
        expect do
          conn.execute("INSERT OR REPLACE INTO offline_test VALUES ('ulid-test-1', 'hello')")
        end.not_to raise_error
      end

      it 'can read locally after write' do
        db   = TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        conn = db.connect
        db.sync
        conn.execute('CREATE TABLE IF NOT EXISTS offline_read_test (id TEXT PRIMARY KEY, val TEXT)')
        conn.execute("INSERT OR REPLACE INTO offline_read_test VALUES ('ulid-1', 'world')")
        result = conn.query("SELECT val FROM offline_read_test WHERE id = 'ulid-1'")
        expect(result).to eq([{ 'val' => 'world' }])
      end

      it 'can push to remote (sync after write)' do
        db   = TursoLibsql::Database.new_synced(offline_path, url, token, 0)
        conn = db.connect
        db.sync
        conn.execute('CREATE TABLE IF NOT EXISTS offline_push_test (id TEXT PRIMARY KEY, val TEXT)')
        conn.execute("INSERT OR REPLACE INTO offline_push_test VALUES ('ulid-push-1', 'pushed')")
        # sync で write をリモートへ push
        expect { db.sync }.not_to raise_error
      end
    end
  end

  # -----------------------------------------------------------------------
  # LibsqlAdapter — AR 経由
  # -----------------------------------------------------------------------

  describe ActiveRecord::ConnectionAdapters::LibsqlAdapter do
    context 'embedded replica mode (online write)' do
      let(:config) do
        { adapter: 'turso', database: url, token: token, replica_path: replica_path }
      end
      let(:adapter) { described_class.new(config) }

      before { adapter.connect! }
      after  { adapter.disconnect! }

      it 'connects without error' do
        expect(adapter).to be_active
      end

      it 'can execute a SELECT query' do
        result = adapter.send(:internal_exec_query, 'SELECT 1 AS n', 'TEST')
        expect(result.rows).to eq([[1]])
      end

      it '#sync pulls from remote without error' do
        expect { adapter.sync }.not_to raise_error
      end

      it 'creates a local replica file' do
        expect(File.exist?(replica_path)).to be true
      end
    end

    context 'offline write mode' do
      let(:config) do
        {
          adapter: 'turso',
          database: url,
          token: token,
          replica_path: offline_path,
          offline: true
        }
      end
      let(:adapter) { described_class.new(config) }

      before { adapter.connect! }
      after  { adapter.disconnect! }

      it 'connects without error' do
        expect(adapter).to be_active
      end

      it 'creates a local db file' do
        expect(File.exist?(offline_path)).to be true
      end

      it '#sync works as pull and push' do
        expect { adapter.sync }.not_to raise_error
      end
    end
  end
end
