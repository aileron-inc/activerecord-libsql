# frozen_string_literal: true

require 'active_record'
require 'active_record/connection_adapters/abstract_adapter'
require 'turso_libsql/turso_libsql'

module ActiveRecord
  module ConnectionHandling
    # database.yml で `adapter: turso` と書いたときに呼ばれる
    def turso_connection(config)
      config = config.symbolize_keys

      url   = config.fetch(:url)   { raise ArgumentError, 'turso adapter requires :url' }
      token = config.fetch(:token) { raise ArgumentError, 'turso adapter requires :token' }

      conn = TursoLibsql::Connection.new(url, token)
      ConnectionAdapters::TursoAdapter.new(conn, logger, config)
    end
  end

  module ConnectionAdapters
    class TursoAdapter < AbstractAdapter
      ADAPTER_NAME = 'Turso'

      # SQLite 互換の型マッピング（libSQL は SQLite 方言）
      NATIVE_DATABASE_TYPES = {
        primary_key: 'INTEGER PRIMARY KEY AUTOINCREMENT',
        string: { name: 'TEXT' },
        text: { name: 'TEXT' },
        integer: { name: 'INTEGER' },
        float: { name: 'REAL' },
        decimal: { name: 'REAL' },
        datetime: { name: 'TEXT' },
        timestamp: { name: 'TEXT' },
        time: { name: 'TEXT' },
        date: { name: 'TEXT' },
        binary: { name: 'BLOB' },
        boolean: { name: 'INTEGER' },
        json: { name: 'TEXT' }
      }.freeze

      def initialize(connection, logger, config)
        super(connection, logger, config)
        @config = config
      end

      # -----------------------------------------------------------------------
      # Adapter 識別
      # -----------------------------------------------------------------------

      def adapter_name
        ADAPTER_NAME
      end

      def supports_migrations?
        true
      end

      def supports_primary_key?
        true
      end

      def supports_ddl_transactions?
        false
      end

      def supports_savepoints?
        false
      end

      def supports_explain?
        false
      end

      def supports_lazy_transactions?
        false
      end

      # -----------------------------------------------------------------------
      # 接続管理
      # -----------------------------------------------------------------------

      def active?
        @connection.query('SELECT 1')
        true
      rescue StandardError
        false
      end

      def reconnect!
        disconnect!
        @connection = TursoLibsql::Connection.new(
          @config[:url],
          @config[:token]
        )
      end

      def disconnect!
        # libsql の Connection は Drop で自動クローズされる
        @connection = nil
      end

      def reset!
        reconnect!
      end

      # -----------------------------------------------------------------------
      # クエリ実行（AR の中核）
      # -----------------------------------------------------------------------

      # SELECT 系: ActiveRecord::Result を返す
      def exec_query(sql, name = nil, _binds = [], prepare: false)
        log(sql, name) do
          rows = @connection.query(sql)

          if rows.empty?
            ActiveRecord::Result.new([], [])
          else
            columns = rows.first.keys
            values  = rows.map(&:values)
            ActiveRecord::Result.new(columns, values)
          end
        end
      end

      # INSERT/UPDATE/DELETE 系
      def exec_update(sql, name = nil, _binds = [])
        log(sql, name) do
          @connection.execute(sql)
        end
      end

      alias exec_delete exec_update

      # INSERT 後の id を返す
      def exec_insert(sql, name = nil, _binds = [], _pk = nil, _sequence_name = nil, returning: nil)
        log(sql, name) do
          @connection.execute(sql)
          @connection.last_insert_rowid
        end
      end

      # -----------------------------------------------------------------------
      # トランザクション
      # -----------------------------------------------------------------------

      def begin_db_transaction
        @connection.begin_transaction
      end

      def commit_db_transaction
        @connection.commit_transaction
      end

      def exec_rollback_db_transaction
        @connection.rollback_transaction
      end

      # -----------------------------------------------------------------------
      # スキーマ情報
      # -----------------------------------------------------------------------

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # テーブル一覧
      def tables(_name = nil)
        result = exec_query(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
          'SCHEMA'
        )
        result.rows.flatten
      end

      # カラム情報
      def columns(table_name)
        result = exec_query("PRAGMA table_info(#{quote_table_name(table_name)})", 'SCHEMA')
        result.map do |row|
          sql_type = row['type'].to_s
          Column.new(
            row['name'],
            row['dflt_value'],
            fetch_type_metadata(sql_type),
            row['notnull'].to_i.zero?
          )
        end
      end

      # テーブルの存在確認
      def table_exists?(table_name)
        tables.include?(table_name.to_s)
      end

      # -----------------------------------------------------------------------
      # DDL（CREATE TABLE / DROP TABLE）
      # -----------------------------------------------------------------------

      def create_table(table_name, **options, &block)
        td = create_table_definition(table_name, **options)
        block.call(td) if block

        sql = schema_creation.accept(td)
        execute(sql)
      end

      def drop_table(table_name, **options)
        execute("DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_table_name(table_name)}")
      end

      # -----------------------------------------------------------------------
      # クォート
      # -----------------------------------------------------------------------

      def quote_column_name(name)
        %("#{name.to_s.gsub('"', '""')}")
      end

      def quote_table_name(name)
        %("#{name.to_s.gsub('"', '""')}")
      end

      def quoted_true
        '1'
      end

      def quoted_false
        '0'
      end

      private

      def initialize_type_map(m = type_map)
        m.register_type(/^integer/i, Type::Integer.new)
        m.register_type(/^real/i,    Type::Float.new)
        m.register_type(/^text/i,    Type::String.new)
        m.register_type(/^blob/i,    Type::Binary.new)
        m.register_type(/^boolean/i, Type::Boolean.new)
        # デフォルトは String
        m.register_type(/./,         Type::String.new)
      end

      def fetch_type_metadata(sql_type)
        cast_type = type_map.lookup(sql_type)
        SqlTypeMetadata.new(
          sql_type: sql_type,
          type: cast_type.type,
          limit: cast_type.limit,
          precision: cast_type.precision,
          scale: cast_type.scale
        )
      end

      def schema_creation
        SchemaCreation.new(self)
      end

      def create_table_definition(name, **options)
        TableDefinition.new(self, name, **options)
      end
    end
  end
end
