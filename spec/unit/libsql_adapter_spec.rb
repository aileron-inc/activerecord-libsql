# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::LibsqlAdapter do
  # TursoLibsql::Database / Connection をモックして、実接続なしでテストする
  let(:mock_connection) { instance_double(TursoLibsql::Connection) }
  let(:mock_database)   { instance_double(TursoLibsql::Database) }
  # AR 8 では AbstractAdapter#initialize に configuration_hash (Hash) を直接渡す
  let(:config_hash) do
    { adapter: 'turso', database: 'libsql://test.turso.io', token: 'test-token' }
  end

  subject(:adapter) do
    allow(TursoLibsql::Database).to receive(:new_remote).and_return(mock_database)
    allow(mock_database).to receive(:connect).and_return(mock_connection)
    described_class.new(config_hash)
  end

  # -----------------------------------------------------------------------
  # Adapter 識別
  # -----------------------------------------------------------------------

  describe '#adapter_name' do
    it 'returns "Turso"' do
      expect(adapter.adapter_name).to eq('Turso')
    end
  end

  describe 'capability flags' do
    it 'supports migrations' do
      expect(adapter.supports_migrations?).to be true
    end

    it 'supports primary key' do
      expect(adapter.supports_primary_key?).to be true
    end

    it 'does not support DDL transactions' do
      expect(adapter.supports_ddl_transactions?).to be false
    end

    it 'does not support savepoints' do
      expect(adapter.supports_savepoints?).to be false
    end
  end

  # -----------------------------------------------------------------------
  # write_query? の判定ロジック
  # -----------------------------------------------------------------------

  describe '#write_query?' do
    it 'returns false for SELECT' do
      expect(adapter.write_query?('SELECT * FROM users')).to be false
    end

    it 'returns false for PRAGMA' do
      expect(adapter.write_query?('PRAGMA table_info("users")')).to be false
    end

    it 'returns false for EXPLAIN' do
      expect(adapter.write_query?('EXPLAIN SELECT 1')).to be false
    end

    it 'returns true for INSERT' do
      expect(adapter.write_query?('INSERT INTO users (name) VALUES (?)')).to be true
    end

    it 'returns true for UPDATE' do
      expect(adapter.write_query?('UPDATE users SET name = ? WHERE id = ?')).to be true
    end

    it 'returns true for DELETE' do
      expect(adapter.write_query?('DELETE FROM users WHERE id = ?')).to be true
    end

    it 'returns true for CREATE TABLE' do
      expect(adapter.write_query?('CREATE TABLE users (id INTEGER PRIMARY KEY)')).to be true
    end

    it 'returns true for DROP TABLE' do
      expect(adapter.write_query?('DROP TABLE users')).to be true
    end
  end

  # -----------------------------------------------------------------------
  # クォート
  # -----------------------------------------------------------------------

  describe '#quote_column_name' do
    it 'wraps name in double quotes' do
      expect(adapter.quote_column_name('name')).to eq('"name"')
    end

    it 'escapes double quotes inside the name' do
      expect(adapter.quote_column_name('col"name')).to eq('"col""name"')
    end
  end

  describe '#quote_table_name' do
    it 'wraps table name in double quotes' do
      expect(adapter.quote_table_name('users')).to eq('"users"')
    end
  end

  describe '#quoted_true / #quoted_false' do
    it 'returns "1" for true' do
      expect(adapter.quoted_true).to eq('1')
    end

    it 'returns "0" for false' do
      expect(adapter.quoted_false).to eq('0')
    end
  end

  # -----------------------------------------------------------------------
  # 接続設定バリデーション
  # -----------------------------------------------------------------------

  describe 'connection config validation' do
    it 'raises ArgumentError when :database is missing' do
      expect do
        described_class.new({ adapter: 'turso', token: 'test-token' }).connect!
      end.to raise_error(ArgumentError, /database/)
    end

    it 'raises ArgumentError when :token is missing (remote mode)' do
      expect do
        described_class.new({ adapter: 'turso', database: 'libsql://test.turso.io', token: nil }).connect!
      end.to raise_error(ArgumentError, /token/)
    end

    it 'does not raise when :token is missing in replica mode (token is optional)' do
      allow(TursoLibsql::Database).to receive(:new_remote_replica).and_return(mock_database)
      allow(mock_database).to receive(:connect).and_return(mock_connection)
      allow(mock_connection).to receive(:query).and_return([{ '1' => 1 }])
      expect do
        described_class.new({
                              adapter: 'turso',
                              database: 'libsql://test.turso.io',
                              replica_path: '/tmp/test_replica.db'
                            }).connect!
      end.not_to raise_error
    end

    it 'uses new_remote_replica when :replica_path is set' do
      expect(TursoLibsql::Database).to receive(:new_remote_replica).with(
        '/tmp/test.db', 'libsql://test.turso.io', 'test-token', 0
      ).and_return(mock_database)
      allow(mock_database).to receive(:connect).and_return(mock_connection)
      allow(mock_connection).to receive(:query).and_return([{ '1' => 1 }])
      described_class.new({
                            adapter: 'turso',
                            database: 'libsql://test.turso.io',
                            token: 'test-token',
                            replica_path: '/tmp/test.db'
                          }).connect!
    end

    it 'passes sync_interval to new_remote_replica' do
      expect(TursoLibsql::Database).to receive(:new_remote_replica).with(
        '/tmp/test.db', 'libsql://test.turso.io', 'test-token', 30
      ).and_return(mock_database)
      allow(mock_database).to receive(:connect).and_return(mock_connection)
      allow(mock_connection).to receive(:query).and_return([{ '1' => 1 }])
      described_class.new({
                            adapter: 'turso',
                            database: 'libsql://test.turso.io',
                            token: 'test-token',
                            replica_path: '/tmp/test.db',
                            sync_interval: 30
                          }).connect!
    end
  end

  # -----------------------------------------------------------------------
  # sync メソッド
  # -----------------------------------------------------------------------

  describe '#sync' do
    it 'delegates to @raw_database' do
      allow(TursoLibsql::Database).to receive(:new_remote).and_return(mock_database)
      allow(mock_database).to receive(:connect).and_return(mock_connection)
      allow(mock_connection).to receive(:query).and_return([{ '1' => 1 }])
      allow(mock_database).to receive(:sync)
      a = described_class.new(config_hash)
      a.connect!
      expect(mock_database).to receive(:sync)
      a.sync
    end

    it 'is a no-op when @raw_database is nil' do
      expect { adapter.sync }.not_to raise_error
    end
  end

  # -----------------------------------------------------------------------
  # perform_query / build_result
  # -----------------------------------------------------------------------

  describe '#perform_query' do
    let(:notification_payload) { {} }

    context 'with a SELECT query' do
      let(:rows) { [{ 'id' => 1, 'name' => 'Alice' }, { 'id' => 2, 'name' => 'Bob' }] }

      before do
        allow(mock_connection).to receive(:query).and_return(rows)
      end

      it 'returns an ActiveRecord::Result' do
        result = adapter.send(:perform_query,
                              mock_connection,
                              'SELECT * FROM users',
                              [],
                              [],
                              prepare: false,
                              notification_payload: notification_payload)
        expect(result).to be_a(ActiveRecord::Result)
        expect(result.columns).to eq(%w[id name])
        expect(result.rows).to eq([[1, 'Alice'], [2, 'Bob']])
      end

      it 'sets row_count in notification_payload' do
        adapter.send(:perform_query,
                     mock_connection,
                     'SELECT * FROM users',
                     [],
                     [],
                     prepare: false,
                     notification_payload: notification_payload)
        expect(notification_payload[:row_count]).to eq(2)
      end
    end

    context 'with an empty SELECT result' do
      before do
        allow(mock_connection).to receive(:query).and_return([])
      end

      it 'returns an empty ActiveRecord::Result' do
        result = adapter.send(:perform_query,
                              mock_connection,
                              'SELECT * FROM users WHERE 1=0',
                              [],
                              [],
                              prepare: false,
                              notification_payload: notification_payload)
        expect(result).to be_a(ActiveRecord::Result)
        expect(result.rows).to be_empty
      end
    end

    context 'with a write query (INSERT)' do
      before do
        allow(mock_connection).to receive(:execute).and_return(1)
      end

      it 'returns an empty ActiveRecord::Result with affected_rows' do
        result = adapter.send(:perform_query,
                              mock_connection,
                              "INSERT INTO users (name) VALUES ('Alice')",
                              [],
                              [],
                              prepare: false,
                              notification_payload: notification_payload)
        expect(result).to be_a(ActiveRecord::Result)
        expect(result.rows).to be_empty
      end
    end

    context 'with bind parameters' do
      before do
        allow(mock_connection).to receive(:query).and_return([{ 'id' => 1 }])
      end

      it 'expands ? placeholders with type_casted_binds' do
        expect(mock_connection).to receive(:query).with('SELECT * FROM users WHERE id = 1')
        adapter.send(:perform_query,
                     mock_connection,
                     'SELECT * FROM users WHERE id = ?',
                     [],
                     [1],
                     prepare: false,
                     notification_payload: notification_payload)
      end
    end
  end

  # -----------------------------------------------------------------------
  # トランザクション
  # -----------------------------------------------------------------------

  describe 'transaction methods' do
    before do
      allow(mock_connection).to receive(:begin_transaction)
      allow(mock_connection).to receive(:commit_transaction)
      allow(mock_connection).to receive(:rollback_transaction)
    end

    it 'delegates begin_db_transaction to raw_connection' do
      adapter.instance_variable_set(:@raw_connection, mock_connection)
      expect(mock_connection).to receive(:begin_transaction)
      adapter.begin_db_transaction
    end

    it 'delegates commit_db_transaction to raw_connection' do
      adapter.instance_variable_set(:@raw_connection, mock_connection)
      expect(mock_connection).to receive(:commit_transaction)
      adapter.commit_db_transaction
    end

    it 'delegates exec_rollback_db_transaction to raw_connection' do
      adapter.instance_variable_set(:@raw_connection, mock_connection)
      expect(mock_connection).to receive(:rollback_transaction)
      adapter.exec_rollback_db_transaction
    end
  end

  # -----------------------------------------------------------------------
  # last_inserted_id
  # -----------------------------------------------------------------------

  describe '#last_inserted_id' do
    it 'delegates to raw_connection.last_insert_rowid' do
      adapter.instance_variable_set(:@raw_connection, mock_connection)
      allow(mock_connection).to receive(:last_insert_rowid).and_return(42)
      expect(adapter.last_inserted_id(nil)).to eq(42)
    end
  end

  # -----------------------------------------------------------------------
  # COLUMN_BUILDER — AR バージョン互換性
  # -----------------------------------------------------------------------

  describe 'COLUMN_BUILDER' do
    # AR 7.x / 8.0: Column.new(name, default, sql_type_metadata, null)
    # AR 8.1+:      Column.new(name, cast_type, default, sql_type_metadata, null)
    # どちらのバージョンでも正しい Column が返ることを検証する

    let(:builder) { described_class.const_get(:COLUMN_BUILDER) }
    let(:cast_type) { ActiveRecord::Type::Integer.new }
    let(:sql_type_md) do
      ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'INTEGER', type: :integer
      )
    end

    subject(:col) { builder.call('age', cast_type, '0', sql_type_md, true) }

    it 'returns a Column instance' do
      expect(col).to be_a(ActiveRecord::ConnectionAdapters::Column)
    end

    it 'sets name correctly' do
      expect(col.name).to eq('age')
    end

    it 'sets default correctly' do
      # AR 8.1+ は cast_type.deserialize で型変換される（'0' → 0）、それ以前は文字列のまま
      expect(col.default).to eq('0').or eq(0)
    end

    it 'sets null correctly' do
      expect(col.null).to be true
    end

    it 'sets sql_type via sql_type_metadata' do
      expect(col.sql_type).to eq('INTEGER')
    end

    it 'does not raise NoMethodError on -@ (AR 8.0 deduplicated bug)' do
      # AR 8.0 では default に cast_type が渡ると -default で NoMethodError が発生していた
      expect { builder.call('age', cast_type, '0', sql_type_md, true) }.not_to raise_error
    end

    it 'handles nil default without error' do
      expect { builder.call('age', cast_type, nil, sql_type_md, true) }.not_to raise_error
    end

    it 'handles not-null column (null: false)' do
      col = builder.call('name', ActiveRecord::Type::String.new, nil, sql_type_md, false)
      expect(col.null).to be false
    end
  end
end
