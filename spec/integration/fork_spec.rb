# frozen_string_literal: true

require 'spec_helper'

# fork 後の接続安全性テスト
# Solid Queue などのマルチプロセス系 gem は Process.fork で子プロセスを作る。
# 親プロセスの Rust オブジェクト（TursoLibsql::Connection）を子プロセスが
# そのまま使うと SEGV するため、discard! → reconnect! の流れを担保する。
RSpec.describe 'fork safety', :integration do
  before(:all) do
    skip 'Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run integration tests' \
      unless ENV['TURSO_DATABASE_URL'] && ENV['TURSO_AUTH_TOKEN']
    skip 'Integration tests skipped (SKIP_INTEGRATION_TESTS=1)' \
      if ENV['SKIP_INTEGRATION_TESTS'] == '1'
    skip 'fork not supported on this platform' unless Process.respond_to?(:fork)

    ActiveRecord::Base.establish_connection(
      adapter: 'turso',
      database: ENV['TURSO_DATABASE_URL'],
      token: ENV['TURSO_AUTH_TOKEN']
    )

    ActiveRecord::Base.connection.create_table(:fork_test_records, force: true) do |t|
      t.string :value, null: false
    end
  end

  after(:all) do
    next unless ActiveRecord::Base.connection.active?

    ActiveRecord::Base.connection.drop_table(:fork_test_records, if_exists: true)
  end

  # -----------------------------------------------------------------------
  # discard! — fork 後に親の接続を安全に破棄できるか
  # -----------------------------------------------------------------------

  describe 'discard!' do
    it 'does not raise when called on an active connection' do
      conn = ActiveRecord::Base.connection
      expect { conn.discard! }.not_to raise_error
    end

    it 'sets raw_connection to nil after discard!' do
      conn = ActiveRecord::Base.connection
      conn.discard!
      expect(conn.instance_variable_get(:@raw_connection)).to be_nil
      expect(conn.instance_variable_get(:@raw_database)).to be_nil
    end

    it 'can reconnect after discard!' do
      conn = ActiveRecord::Base.connection
      conn.discard!
      conn.reconnect!
      expect(conn).to be_active
    end
  end

  # -----------------------------------------------------------------------
  # fork — 子プロセスで新しい接続を確立してクエリを実行できるか
  # -----------------------------------------------------------------------

  describe 'Process.fork' do
    # fork 後の正しいパターン:
    # 1. pool.discard! で親の Rust オブジェクトへの参照を破棄
    # 2. establish_connection で新しい接続を確立
    def child_setup
      ActiveRecord::Base.connection_handler.each_connection_pool(&:discard!)
      ActiveRecord::Base.establish_connection(
        adapter: 'turso',
        database: ENV['TURSO_DATABASE_URL'],
        token: ENV['TURSO_AUTH_TOKEN']
      )
    end

    it 'child process can query DB independently without SEGV' do
      ActiveRecord::Base.connection.execute('SELECT 1')

      rd, wr = IO.pipe
      pid = fork do
        rd.close
        begin
          child_setup
          ActiveRecord::Base.connection.execute('SELECT 42 AS n')
          wr.write('ok')
        rescue StandardError => e
          wr.write("error:#{e.class}:#{e.message}")
        ensure
          wr.close
          exit!(0)
        end
      end

      wr.close
      output = rd.read
      rd.close
      Process.waitpid(pid)

      expect(output).to eq('ok')
    end

    it 'child process can INSERT and SELECT without SEGV' do
      rd, wr = IO.pipe
      pid = fork do
        rd.close
        begin
          child_setup
          ActiveRecord::Base.connection.execute(
            "INSERT INTO fork_test_records (value) VALUES ('from-child-#{Process.pid}')"
          )
          rows = ActiveRecord::Base.connection.execute(
            "SELECT value FROM fork_test_records WHERE value LIKE 'from-child-%'"
          )
          wr.write(rows.any? ? 'ok' : 'empty')
        rescue StandardError => e
          wr.write("error:#{e.class}:#{e.message}")
        ensure
          wr.close
          exit!(0)
        end
      end

      wr.close
      output = rd.read
      rd.close
      Process.waitpid(pid)

      expect(output).to eq('ok')
    end

    it 'multiple child processes can query concurrently without SEGV' do
      pipes = 3.times.map { IO.pipe }
      pids = pipes.map do |rd, wr|
        fork do
          rd.close
          begin
            child_setup
            ActiveRecord::Base.connection.execute('SELECT 1')
            wr.write('ok')
          rescue StandardError => e
            wr.write("error:#{e.class}")
          ensure
            wr.close
            exit!(0)
          end
        end
      end

      results = pipes.map do |rd, wr|
        wr.close
        out = rd.read
        rd.close
        out
      end
      pids.each { |pid| Process.waitpid(pid) }

      expect(results).to all(eq('ok'))
    end
  end

  # -----------------------------------------------------------------------
  # 親プロセスは fork 後も引き続き使えるか
  # -----------------------------------------------------------------------

  describe 'parent process after fork' do
    it 'parent can still query after child exits' do
      pid = fork do
        ActiveRecord::Base.connection_handler.each_connection_pool(&:discard!)
        exit!(0)
      end
      Process.waitpid(pid)

      expect { ActiveRecord::Base.connection.execute('SELECT 1') }.not_to raise_error
    end
  end
end
