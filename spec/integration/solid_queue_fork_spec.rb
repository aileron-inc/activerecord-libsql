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
# 実際の Solid Queue の起動フロー:
#   Supervisor（親）が AR 接続を確立した状態で fork()
#   → ActiveSupport::ForkTracker が子プロセスで PoolConfig.discard_pools! を自動呼び出し
#   → Rails executor（wrap_in_app_executor）が establish_connection 相当の処理を行う
#   → 子プロセスが reconnect して SolidQueue::Process.register → INSERT → COMMIT
#
# このスペックは Rails executor の代わりに establish_connection を呼ぶことで
# 実際の Solid Queue の動作を再現する。
# （ForkTracker による discard_pools! は fork するだけで自動的に発生する）
RSpec.describe 'Solid Queue fork simulation', :integration do
  # テスト全体で使う接続設定を保持
  let(:db_config) do
    {
      adapter: 'turso',
      database: ENV['TURSO_DATABASE_URL'],
      token: ENV['TURSO_AUTH_TOKEN']
    }
  end

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
  # 実際の動作:
  #   1. fork() → ActiveSupport::ForkTracker が子プロセスで
  #               PoolConfig.discard_pools! を自動呼び出し（pool が discarded? 状態に）
  #   2. Rails executor の wrap が establish_connection 相当の処理を行い
  #      新しい pool を作成する
  #   3. 子プロセスが通常通り AR を使って動作する
  #
  # テストでは Rails executor の代わりに establish_connection を直接呼ぶ。
  # -----------------------------------------------------------------------

  def simulate_solid_queue_worker
    rd, wr = IO.pipe

    pid = fork do
      rd.close
      begin
        # ForkTracker が自動で discard_pools! を呼んだ後、
        # Rails executor（wrap_in_app_executor）が行う接続再確立を再現する。
        # 実際の Solid Queue は app.executor.wrap 経由でこれが行われる。
        ActiveRecord::Base.establish_connection(
          adapter: 'turso',
          database: ENV['TURSO_DATABASE_URL'],
          token: ENV['TURSO_AUTH_TOKEN']
        )

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

  it 'child process can INSERT after fork' do
    # 親プロセスで接続を確立してクエリを実行（接続が open な状態で fork する）
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

  it 'child process can call SolidQueue::Process.create! after fork' do
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

  it 'child process can run FOR UPDATE SKIP LOCKED query after fork' do
    # Dispatcher 用のスケジュール済みジョブを作成
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
      # Solid Queue の Dispatcher が実際に発行するクエリ
      # Solid Queue の Dispatcher が実際に発行するクエリ
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

  it 'multiple forked workers can all INSERT without errors' do
    ActiveRecord::Base.connection.execute('SELECT 1')

    pipes = 3.times.map { IO.pipe }
    pids = pipes.map do |_rd, wr|
      fork do
        _rd.close
        begin
          # Rails executor の代わりに establish_connection を呼ぶ
          ActiveRecord::Base.establish_connection(
            adapter: 'turso',
            database: ENV['TURSO_DATABASE_URL'],
            token: ENV['TURSO_AUTH_TOKEN']
          )

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
      ActiveRecord::Base.establish_connection(
        adapter: 'turso',
        database: ENV['TURSO_DATABASE_URL'],
        token: ENV['TURSO_AUTH_TOKEN']
      )
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
end
