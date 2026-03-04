# frozen_string_literal: true

require 'active_record'
require 'active_record/connection_adapters/abstract_adapter'
require 'turso_libsql/turso_libsql'

# AR 7.2+ のアダプター登録 API
ActiveSupport.on_load(:active_record) do
  ActiveRecord::ConnectionAdapters.register(
    'turso',
    'ActiveRecord::ConnectionAdapters::LibsqlAdapter',
    'active_record/connection_adapters/libsql_adapter'
  )
end

module ActiveRecord
  module ConnectionAdapters
    class LibsqlAdapter < AbstractAdapter
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

      # SQLite 互換: PRAGMA も読み取りクエリとして扱う
      READ_QUERY = ActiveRecord::ConnectionAdapters::AbstractAdapter.build_read_query_regexp(
        :pragma
      )
      private_constant :READ_QUERY

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

      def write_query?(sql)
        !READ_QUERY.match?(sql)
      rescue ArgumentError
        !READ_QUERY.match?(sql.b)
      end

      # -----------------------------------------------------------------------
      # 接続管理（AR 8 スタイル）
      # @raw_connection に TursoLibsql::Connection をセットする
      # AR の ConnectionPool はスレッドごとに独立した Adapter インスタンスを払い出すため
      # @raw_connection の競合は発生しない
      # -----------------------------------------------------------------------

      def connect!
        @raw_connection = build_libsql_connection
        super
      end

      def active?
        return false unless @raw_connection

        @raw_connection.query('SELECT 1')
        true
      rescue StandardError
        false
      end

      def reconnect!
        @raw_connection = build_libsql_connection
        super
      end

      def disconnect!
        @raw_connection = nil
        super
      end

      # -----------------------------------------------------------------------
      # AR 8 クエリパイプライン
      # raw_execute → perform_query → cast_result の流れ
      # -----------------------------------------------------------------------

      # AR 8 が with_raw_connection { |conn| } で呼ぶ中核メソッド
      def perform_query(raw_connection, sql, _binds, type_casted_binds, prepare:, notification_payload:, batch: false)
        # バインドパラメータを SQL に展開する（libsql の ? プレースホルダーに対応）
        expanded_sql = if type_casted_binds&.any?
                         i = -1
                         sql.gsub('?') do
                           i += 1
                           quote(type_casted_binds[i])
                         end
                       else
                         sql
                       end

        if read_query?(expanded_sql)
          rows = raw_connection.query(expanded_sql)
          notification_payload[:row_count] = rows.size if notification_payload
          build_result(rows)
        else
          affected = raw_connection.execute(expanded_sql)
          notification_payload[:row_count] = affected if notification_payload
          ActiveRecord::Result.empty(affected_rows: affected.to_i)
        end
      end

      # perform_query が返した結果をそのまま使う（すでに ActiveRecord::Result）
      def cast_result(raw_result)
        raw_result
      end

      def affected_rows(raw_result)
        raw_result.length
      end

      # -----------------------------------------------------------------------
      # トランザクション
      # -----------------------------------------------------------------------

      def begin_db_transaction
        @raw_connection&.begin_transaction
      end

      def commit_db_transaction
        @raw_connection&.commit_transaction
      end

      def exec_rollback_db_transaction
        @raw_connection&.rollback_transaction
      end

      # -----------------------------------------------------------------------
      # INSERT 後の id
      # AR 8 は last_inserted_id(result) を呼ぶ
      # -----------------------------------------------------------------------

      def last_inserted_id(_result)
        @raw_connection&.last_insert_rowid
      end

      # -----------------------------------------------------------------------
      # スキーマ情報
      # -----------------------------------------------------------------------

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      def tables(_name = nil)
        result = internal_exec_query(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
          'SCHEMA'
        )
        result.rows.flatten
      end

      def columns(table_name)
        result = internal_exec_query(
          "PRAGMA table_info(#{quote_table_name(table_name)})",
          'SCHEMA'
        )
        result.map do |row|
          sql_type    = row['type'].to_s
          cast_type   = type_map.lookup(sql_type) || Type::Value.new
          sql_type_md = fetch_type_metadata(sql_type)
          # AR 8.1: Column.new(name, cast_type, default, sql_type_metadata, null)
          Column.new(
            row['name'],
            cast_type,
            row['dflt_value'],
            sql_type_md,
            row['notnull'].to_i.zero?
          )
        end
      end

      def table_exists?(table_name)
        tables.include?(table_name.to_s)
      end

      # -----------------------------------------------------------------------
      # DDL
      # -----------------------------------------------------------------------

      def create_table(table_name, **options, &block)
        td = create_table_definition(table_name, **options)
        block.call(td) if block
        execute(schema_creation.accept(td))
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

      def build_libsql_connection
        database_url = @config[:database] || @config[:url]
        raise ArgumentError, 'libsql adapter requires :database (libsql://...)' unless database_url

        token = @config[:token]
        raise ArgumentError, 'libsql adapter requires :token' unless token

        TursoLibsql::Connection.new(database_url.to_s, token.to_s)
      end

      # PK 取得（PRAGMA table_info の pk カラムを使う）
      def primary_keys(table_name)
        result = internal_exec_query(
          "PRAGMA table_info(#{quote_table_name(table_name)})",
          'SCHEMA'
        )
        pks = result.select { |row| row['pk'].to_i > 0 }
        pks.sort_by { |row| row['pk'].to_i }.map { |row| row['name'] }
      end

      # AR が views / data_sources で使う（SQLite 互換実装）
      def data_source_sql(name = nil, type: nil)
        scope = quoted_scope(name, type: type)
        scope[:type] ||= "'table','view'"

        sql = +"SELECT name FROM pragma_table_list WHERE schema <> 'temp'"
        sql << " AND name NOT IN ('sqlite_sequence', 'sqlite_schema')"
        sql << " AND name = #{scope[:name]}" if scope[:name]
        sql << " AND type IN (#{scope[:type]})"
        sql
      end

      def quoted_scope(name = nil, type: nil)
        type = case type
               when 'BASE TABLE'    then "'table'"
               when 'VIEW'          then "'view'"
               when 'VIRTUAL TABLE' then "'virtual'"
               end
        scope = {}
        scope[:name] = quote(name) if name
        scope[:type] = type if type
        scope
      end

      # SELECT 系クエリかどうかを判定
      def read_query?(sql)
        sql.lstrip.match?(/\A\s*(SELECT|PRAGMA|EXPLAIN|WITH)\b/i)
      end

      # Array of Hash → ActiveRecord::Result
      def build_result(rows)
        return ActiveRecord::Result.new([], []) if rows.empty?

        columns = rows.first.keys
        values  = rows.map(&:values)
        ActiveRecord::Result.new(columns, values)
      end

      def initialize_type_map(m = type_map)
        m.register_type(/^integer/i, Type::Integer.new)
        m.register_type(/^real/i,    Type::Float.new)
        m.register_type(/^text/i,    Type::String.new)
        m.register_type(/^blob/i,    Type::Binary.new)
        m.register_type(/^boolean/i, Type::Boolean.new)
        m.register_type(/./,         Type::String.new)
      end

      def fetch_type_metadata(sql_type)
        cast_type = type_map.lookup(sql_type) || Type::Value.new
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
