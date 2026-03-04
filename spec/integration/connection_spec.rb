# frozen_string_literal: true

require 'spec_helper'

# 実接続テスト: TURSO_DATABASE_URL と TURSO_AUTH_TOKEN が必要
# CI 環境では SKIP_INTEGRATION_TESTS=1 で全スキップ可能
RSpec.describe TursoLibsql::Connection, :integration do
  before(:all) do
    skip 'Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run integration tests' \
      unless ENV['TURSO_DATABASE_URL'] && ENV['TURSO_AUTH_TOKEN']
    skip 'Integration tests skipped (SKIP_INTEGRATION_TESTS=1)' \
      if ENV['SKIP_INTEGRATION_TESTS'] == '1'
  end

  let(:url)   { ENV['TURSO_DATABASE_URL'] }
  let(:token) { ENV['TURSO_AUTH_TOKEN'] }

  subject(:conn) { described_class.new(url, token) }

  # -----------------------------------------------------------------------
  # 接続
  # -----------------------------------------------------------------------

  describe '.new' do
    it 'establishes a connection without error' do
      expect { conn }.not_to raise_error
    end

    it 'raises on invalid URL when querying' do
      # libsql は遅延接続のため、.new 時ではなく実際のクエリ実行時にエラーになる
      bad_conn = described_class.new('libsql://invalid.example.invalid', 'bad-token')
      expect do
        bad_conn.query('SELECT 1')
      end.to raise_error(RuntimeError)
    end
  end

  # -----------------------------------------------------------------------
  # query (SELECT)
  # -----------------------------------------------------------------------

  describe '#query' do
    it 'returns an array of hashes for SELECT 1' do
      result = conn.query('SELECT 1 AS ping')
      expect(result).to be_an(Array)
      expect(result.first).to eq({ 'ping' => 1 })
    end

    it 'returns an empty array for no rows' do
      conn.execute("CREATE TABLE IF NOT EXISTS _spec_empty_#{Process.pid} (id INTEGER)")
      result = conn.query("SELECT * FROM _spec_empty_#{Process.pid}")
      expect(result).to eq([])
    ensure
      conn.execute("DROP TABLE IF EXISTS _spec_empty_#{Process.pid}")
    end

    it 'raises on invalid SQL' do
      expect { conn.query('NOT VALID SQL!!!') }.to raise_error(RuntimeError)
    end
  end

  # -----------------------------------------------------------------------
  # execute (INSERT / UPDATE / DELETE)
  # -----------------------------------------------------------------------

  describe '#execute' do
    let(:table) { "_spec_conn_#{Process.pid}" }

    before do
      conn.execute("CREATE TABLE IF NOT EXISTS #{table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    end

    after do
      conn.execute("DROP TABLE IF EXISTS #{table}")
    end

    it 'returns affected row count for INSERT' do
      affected = conn.execute("INSERT INTO #{table} (name) VALUES ('Alice')")
      expect(affected).to eq(1)
    end

    it 'returns affected row count for UPDATE' do
      conn.execute("INSERT INTO #{table} (name) VALUES ('Bob')")
      affected = conn.execute("UPDATE #{table} SET name = 'Robert' WHERE name = 'Bob'")
      expect(affected).to eq(1)
    end

    it 'returns affected row count for DELETE' do
      conn.execute("INSERT INTO #{table} (name) VALUES ('Charlie')")
      affected = conn.execute("DELETE FROM #{table} WHERE name = 'Charlie'")
      expect(affected).to eq(1)
    end
  end

  # -----------------------------------------------------------------------
  # last_insert_rowid
  # -----------------------------------------------------------------------

  describe '#last_insert_rowid' do
    let(:table) { "_spec_rowid_#{Process.pid}" }

    before do
      conn.execute("CREATE TABLE IF NOT EXISTS #{table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    end

    after do
      conn.execute("DROP TABLE IF EXISTS #{table}")
    end

    it 'returns the rowid of the last inserted row' do
      conn.execute("INSERT INTO #{table} (name) VALUES ('Alice')")
      rowid = conn.last_insert_rowid
      expect(rowid).to be_a(Integer)
      expect(rowid).to be > 0
    end
  end

  # -----------------------------------------------------------------------
  # トランザクション
  # -----------------------------------------------------------------------

  describe 'transactions' do
    let(:table) { "_spec_tx_#{Process.pid}" }

    before do
      conn.execute("CREATE TABLE IF NOT EXISTS #{table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    end

    after do
      conn.execute("DROP TABLE IF EXISTS #{table}")
    end

    it 'commits a transaction' do
      conn.begin_transaction
      conn.execute("INSERT INTO #{table} (name) VALUES ('committed')")
      conn.commit_transaction

      rows = conn.query("SELECT name FROM #{table}")
      expect(rows.map { |r| r['name'] }).to include('committed')
    end

    it 'rolls back a transaction' do
      conn.begin_transaction
      conn.execute("INSERT INTO #{table} (name) VALUES ('rolled_back')")
      conn.rollback_transaction

      rows = conn.query("SELECT name FROM #{table}")
      expect(rows.map { |r| r['name'] }).not_to include('rolled_back')
    end
  end
end
