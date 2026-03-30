# frozen_string_literal: true

require 'spec_helper'
require 'rails'
require 'active_job'
require 'active_support/all'
require 'socket'

# Rails スタブ（Solid Queue は Rails::Engine を継承しているため必要）
unless defined?(Rails.application) && Rails.application
  module Rails
    class Application < Engine; end
  end

  class SolidQueueForkTestApp < Rails::Application
    config.eager_load = false
    config.active_support.deprecation = :silence
  end

  Rails.application = SolidQueueForkTestApp.new
  Rails.application.initialize!
end

require 'solid_queue'

_sq_root = Gem.loaded_specs['solid_queue']&.gem_dir ||
           File.expand_path('../../../../..', Gem.find_files('solid_queue.rb').first)
_sq_models = File.join(_sq_root, 'app', 'models')

$solid_queue_zeitwerk_registered ||= false
unless $solid_queue_zeitwerk_registered
  _loader = Zeitwerk::Loader.new
  _loader.push_dir(_sq_models, namespace: Object)
  _loader.setup
  $solid_queue_zeitwerk_registered = true
end

# Solid Queue fork シミュレーションテスト
#
# 実際の Solid Queue の起動フロー（Runnable#run_in_mode）:
#   fork do
#     boot   # → run_callbacks(:boot) → register → wrap_in_app_executor { Process.create! }
#     run    # → polling loop
#   end
#
# fork 後の AR 接続管理:
#   1. ActiveSupport::ForkTracker が子プロセスで PoolConfig.discard_pools! を自動呼び出し
#      → 各 PoolConfig の @pool = nil（pool が破棄される）
#   2. 子プロセスが AR を使うと connection_handler → pool_config.pool が呼ばれる
#   3. pool_config.pool は @pool が nil なら新しい ConnectionPool を自動作成する
#   4. establish_connection は不要 — AR 8 が自動で pool を再作成する
#
# このスペックは establish_connection を呼ばず、AR の自動 pool 再作成に任せることで
# 実際の Solid Queue の動作を正確に再現する。
RSpec.describe 'Solid Queue fork simulation', :integration do
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

    conn = ActiveRecord::Base.connection

    conn.create_table :solid_queue_jobs, force: true do |t|
      t.string   :queue_name,    null: false
      t.string   :class_name,    null: false
      t.text     :arguments
      t.integer  :priority, default: 0, null: false
      t.string   :active_job_id
      t.datetime :scheduled_at
      t.datetime :finished_at
      t.string   :concurrency_key
      t.datetime :created_at,    null: false
      t.datetime :updated_at,    null: false
    end

    conn.create_table :solid_queue_processes, force: true do |t|
      t.string   :kind,              null: false
      t.datetime :last_heartbeat_at, null: false
      t.bigint   :supervisor_id
      t.integer  :pid, null: false
      t.string   :hostname
      t.text     :metadata
      t.datetime :created_at,        null: false
      t.string   :name,              null: false
    end

    conn.create_table :solid_queue_ready_executions, force: true do |t|
      t.bigint   :job_id,     null: false
      t.string   :queue_name, null: false
      t.integer  :priority,   default: 0, null: false
      t.datetime :created_at, null: false
    end

    conn.create_table :solid_queue_scheduled_executions, force: true do |t|
      t.bigint   :job_id,       null: false
      t.string   :queue_name,   null: false
      t.integer  :priority,     default: 0, null: false
      t.datetime :scheduled_at, null: false
      t.datetime :created_at,   null: false
    end

    conn.create_table :solid_queue_claimed_executions, force: true do |t|
      t.bigint   :job_id, null: false
      t.bigint   :process_id
      t.datetime :created_at, null: false
    end

    conn.create_table :solid_queue_failed_executions, force: true do |t|
      t.bigint   :job_id,     null: false
      t.text     :error
      t.datetime :created_at, null: false
    end

    conn.create_table :solid_queue_blocked_executions, force: true do |t|
      t.bigint   :job_id,          null: false
      t.string   :queue_name,      null: false
      t.integer  :priority,        default: 0, null: false
      t.string   :concurrency_key, null: false
      t.datetime :expires_at,      null: false
      t.datetime :created_at,      null: false
    end

    conn.create_table :solid_queue_recurring_executions, force: true do |t|
      t.bigint   :job_id,     null: false
      t.string   :task_key,   null: false
      t.datetime :run_at,     null: false
      t.datetime :created_at, null: false
    end

    conn.create_table :solid_queue_semaphores, force: true do |t|
      t.string   :key,        null: false
      t.integer  :value,      default: 1, null: false
      t.datetime :expires_at, null: false
      t.datetime :created_at, null: false
      t.datetime :updated_at, null: false
    end
  end

  after(:all) do
    next unless ActiveRecord::Base.connection.active?

    conn = ActiveRecord::Base.connection
    %w[
      solid_queue_semaphores
      solid_queue_recurring_executions
      solid_queue_blocked_executions
      solid_queue_failed_executions
      solid_queue_claimed_executions
      solid_queue_scheduled_executions
      solid_queue_ready_executions
      solid_queue_processes
      solid_queue_jobs
    ].each { |t| conn.drop_table(t, if_exists: true) }
  end

  # -----------------------------------------------------------------------
  # Solid Queue の実際の起動フローを再現するヘルパー
  #
  # Solid Queue の Runnable#run_in_mode:
  #   fork do
  #     boot; run
  #   end
  #
  # establish_connection は呼ばない。
  # ForkTracker が discard_pools! を自動呼び出し → AR が pool を自動再作成する。
  # -----------------------------------------------------------------------

  def simulate_solid_queue_worker
    rd, wr = IO.pipe

    pid = fork do
      rd.close
      begin
        # establish_connection は呼ばない。
        # ActiveSupport::ForkTracker が fork 後に PoolConfig.discard_pools! を自動呼び出し、
        # @pool = nil にする。その後 AR が connection を要求すると pool_config.pool が
        # 新しい ConnectionPool を自動作成する（AR 8 の設計）。
        yield wr
      rescue StandardError => e
        wr.write("error:#{e.class}:#{e.message}\n#{e.backtrace.first(5).join("\n")}")
      ensure
        wr.close
        exit!(0)
      end
    end

    wr.close
    output = rd.read
    rd.close
    Process.waitpid(pid)
    output
  end

  # -----------------------------------------------------------------------
  # 基本的な INSERT → COMMIT が fork 後に動くか
  # -----------------------------------------------------------------------

  it 'child process can INSERT after fork without establish_connection' do
    ActiveRecord::Base.connection.execute('SELECT 1')

    output = simulate_solid_queue_worker do |wr|
      ActiveRecord::Base.connection.execute(
        'INSERT INTO solid_queue_jobs (queue_name, class_name, arguments, priority, created_at, updated_at) ' \
        "VALUES ('default', 'ForkTestJob', '[]', 0, datetime('now'), datetime('now'))"
      )
      wr.write('ok')
    end

    expect(output).to eq('ok'), "child process failed: #{output}"
  end

  # -----------------------------------------------------------------------
  # SolidQueue::Process.create! — Supervisor 起動時の実際の呼び出し
  # -----------------------------------------------------------------------

  it 'child process can call SolidQueue::Process.create! after fork without establish_connection' do
    ActiveRecord::Base.connection.execute('SELECT 1')

    output = simulate_solid_queue_worker do |wr|
      process = SolidQueue::Process.create!(
        kind: 'Worker',
        last_heartbeat_at: Time.now,
        pid: Process.pid,
        hostname: Socket.gethostname,
        name: "worker-fork-test-#{Process.pid}-#{SecureRandom.hex(4)}"
      )
      wr.write(process.id.to_s)
    end

    expect(output).to match(/\A\d+\z/), "expected process id, got: #{output}"
  end

  # -----------------------------------------------------------------------
  # FOR UPDATE SKIP LOCKED — Dispatcher が発行するクエリ
  # -----------------------------------------------------------------------

  it 'child process can run FOR UPDATE SKIP LOCKED query after fork without establish_connection' do
    job = SolidQueue::Job.create!(
      queue_name: 'default',
      class_name: 'ForkDispatcherTestJob',
      arguments: '[]',
      priority: 0,
      scheduled_at: Time.now - 1
    )
    SolidQueue::ScheduledExecution.create!(
      job_id: job.id,
      queue_name: job.queue_name,
      priority: job.priority,
      scheduled_at: job.scheduled_at
    )

    output = simulate_solid_queue_worker do |wr|
      ids = SolidQueue::ScheduledExecution
            .where('scheduled_at <= ?', Time.now)
            .order(:scheduled_at, :priority, :job_id)
            .limit(500)
            .lock('FOR UPDATE SKIP LOCKED')
            .pluck(:job_id)
      wr.write(ids.any? ? 'ok' : 'empty')
    end

    expect(output).to eq('ok'), "FOR UPDATE SKIP LOCKED failed in child: #{output}"
  end

  # -----------------------------------------------------------------------
  # 複数ワーカーが同時に fork する — Supervisor の実際の動作
  # -----------------------------------------------------------------------

  it 'multiple forked workers can all INSERT without establish_connection' do
    ActiveRecord::Base.connection.execute('SELECT 1')

    pipes = 3.times.map { IO.pipe }
    pids = pipes.map do |_rd, wr|
      fork do
        _rd.close
        begin
          SolidQueue::Process.create!(
            kind: 'Worker',
            last_heartbeat_at: Time.now,
            pid: Process.pid,
            hostname: Socket.gethostname,
            name: "worker-multi-#{Process.pid}-#{SecureRandom.hex(4)}"
          )
          wr.write('ok')
        rescue StandardError => e
          wr.write("error:#{e.class}:#{e.message[0, 200]}")
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

    expect(results).to all(eq('ok')), "some workers failed: #{results}"
  end

  # -----------------------------------------------------------------------
  # 親プロセスは fork 後も引き続き動作するか
  # -----------------------------------------------------------------------

  it 'parent process continues to work after child forks' do
    pid = fork do
      # 子プロセスは何もせず終了（ForkTracker の discard_pools! だけ発生させる）
      exit!(0)
    end
    Process.waitpid(pid)

    expect do
      SolidQueue::Job.create!(
        queue_name: 'default',
        class_name: 'ParentAfterForkJob',
        arguments: '[]',
        priority: 0
      )
    end.not_to raise_error
  end

  # -----------------------------------------------------------------------
  # Embedded Replica（LocalConnection）モード
  #
  # seamless 本番環境は replica_path を使う LocalConnection で動く。
  # remote モードのテストだけでは以下の問題を検出できなかった:
  #   - WAL モード未設定 → 複数 fork が同時書き込みで database is locked
  #   - busy_timeout 未設定 → ロック競合で即エラー
  # -----------------------------------------------------------------------

  context 'Embedded Replica mode (LocalConnection)' do
    let(:tmpdir)       { Dir.mktmpdir('solid_queue_replica_spec') }
    let(:replica_path) { File.join(tmpdir, 'queue.sqlite3') }

    after do
      begin
        ActiveRecord::Base.remove_connection
      rescue StandardError
        nil
      end
      FileUtils.remove_entry(tmpdir) if File.exist?(tmpdir)
      # remote モードに戻す
      ActiveRecord::Base.establish_connection(
        adapter: 'turso',
        database: ENV['TURSO_DATABASE_URL'],
        token: ENV['TURSO_AUTH_TOKEN']
      )
    end

    def establish_replica_connection(path)
      ActiveRecord::Base.establish_connection(
        adapter: 'turso',
        database: ENV['TURSO_DATABASE_URL'],
        token: ENV['TURSO_AUTH_TOKEN'],
        replica_path: path,
        sync_interval: 0
      )
    end

    def setup_replica_schema(path)
      establish_replica_connection(path)
      conn = ActiveRecord::Base.connection

      conn.create_table :solid_queue_jobs, force: true do |t|
        t.string   :queue_name, null: false
        t.string   :class_name, null: false
        t.text     :arguments
        t.integer  :priority, default: 0, null: false
        t.string   :active_job_id
        t.datetime :scheduled_at
        t.datetime :finished_at
        t.string   :concurrency_key
        t.datetime :created_at, null: false
        t.datetime :updated_at, null: false
      end

      conn.create_table :solid_queue_processes, force: true do |t|
        t.string   :kind,              null: false
        t.datetime :last_heartbeat_at, null: false
        t.bigint   :supervisor_id
        t.integer  :pid, null: false
        t.string   :hostname
        t.text     :metadata
        t.datetime :created_at,        null: false
        t.string   :name,              null: false
      end
    end

    # WAL モードが有効になっているか
    # WAL なしだと複数 fork の同時書き込みで database is locked が発生する
    it 'LocalConnection opens SQLite in WAL mode' do
      setup_replica_schema(replica_path)

      require 'sqlite3'
      db = SQLite3::Database.new(replica_path)
      db.results_as_hash = true
      journal_mode = db.execute('PRAGMA journal_mode').first['journal_mode']
      db.close

      expect(journal_mode).to eq('wal'),
                              "Expected WAL mode but got '#{journal_mode}'. " \
                              'Without WAL, concurrent fork writes cause "database is locked".'
    end

    # busy_timeout が設定されているか
    # busy_timeout は接続ごとの設定でファイルに永続化されない。
    # LocalConnection の @db に直接アクセスして確認する。
    it 'LocalConnection sets busy_timeout on its internal connection' do
      setup_replica_schema(replica_path)

      # AR の raw_connection が LocalConnection であることを確認
      raw = ActiveRecord::Base.connection.instance_variable_get(:@raw_connection)
      expect(raw).to be_a(TursoLibsql::LocalConnection)

      # LocalConnection の内部 SQLite3::Database に直接 PRAGMA を問い合わせる
      internal_db = raw.instance_variable_get(:@db)
      timeout = internal_db.execute('PRAGMA busy_timeout').first['timeout']

      expect(timeout.to_i).to be > 0,
                              'Expected busy_timeout > 0. Without it, lock contention causes immediate BusyException.'
    end

    # fork 後に LocalConnection で INSERT できるか
    it 'child process can INSERT via LocalConnection after fork' do
      setup_replica_schema(replica_path)
      ActiveRecord::Base.connection.execute('SELECT 1')

      rd, wr = IO.pipe
      pid = fork do
        rd.close
        begin
          SolidQueue::Process.create!(
            kind: 'Worker',
            last_heartbeat_at: Time.now,
            pid: Process.pid,
            hostname: Socket.gethostname,
            name: "worker-replica-fork-#{Process.pid}-#{SecureRandom.hex(4)}"
          )
          wr.write('ok')
        rescue StandardError => e
          wr.write("error:#{e.class}:#{e.message[0, 300]}")
        ensure
          wr.close
          exit!(0)
        end
      end

      wr.close
      output = rd.read
      rd.close
      Process.waitpid(pid)

      expect(output).to eq('ok'), "child INSERT via LocalConnection failed: #{output}"
    end

    # 複数 fork が同時に同じ .sqlite3 に書いても database is locked にならないか
    # これが今回の bin/jobs エラーの直接再現
    it 'multiple forked workers can INSERT concurrently without database is locked' do
      setup_replica_schema(replica_path)
      ActiveRecord::Base.connection.execute('SELECT 1')

      pipes = 5.times.map { IO.pipe }
      pids = pipes.map do |_rd, wr|
        fork do
          _rd.close
          begin
            SolidQueue::Process.create!(
              kind: 'Worker',
              last_heartbeat_at: Time.now,
              pid: Process.pid,
              hostname: Socket.gethostname,
              name: "worker-concurrent-#{Process.pid}-#{SecureRandom.hex(4)}"
            )
            wr.write('ok')
          rescue StandardError => e
            wr.write("error:#{e.class}:#{e.message[0, 200]}")
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

      expect(results).to all(eq('ok')),
                         "Some workers got 'database is locked': #{results.reject { |r| r == 'ok' }}"
    end
  end
end
