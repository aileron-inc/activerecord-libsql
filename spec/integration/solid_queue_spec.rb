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

  class SolidQueueTestApp < Rails::Application
    config.eager_load = false
    config.active_support.deprecation = :silence
  end

  Rails.application = SolidQueueTestApp.new
  Rails.application.initialize!
end

require 'solid_queue'

# Solid Queue の app/models を Zeitwerk autoload に登録
_sq_root = Gem.loaded_specs['solid_queue']&.gem_dir ||
           File.expand_path('../../../../..', Gem.find_files('solid_queue.rb').first)
_sq_models = File.join(_sq_root, 'app', 'models')

$solid_queue_loader_registered ||= false
unless $solid_queue_loader_registered
  _loader = Zeitwerk::Loader.new
  _loader.push_dir(_sq_models, namespace: Object)
  _loader.setup
  $solid_queue_loader_registered = true
end

# Solid Queue 統合テスト
# TURSO_DATABASE_URL と TURSO_AUTH_TOKEN が必要
# CI 環境では SKIP_INTEGRATION_TESTS=1 で全スキップ可能
RSpec.describe 'Solid Queue integration', :integration do
  before(:all) do
    skip 'Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run integration tests' \
      unless ENV['TURSO_DATABASE_URL'] && ENV['TURSO_AUTH_TOKEN']
    skip 'Integration tests skipped (SKIP_INTEGRATION_TESTS=1)' \
      if ENV['SKIP_INTEGRATION_TESTS'] == '1'

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
  # SolidQueue::Job — INSERT が動くか（エラーの根本だった箇所）
  # -----------------------------------------------------------------------

  describe SolidQueue::Job do
    it 'creates a job record' do
      job = SolidQueue::Job.create!(
        queue_name: 'default',
        class_name: 'TestJob',
        arguments: '[]',
        priority: 0
      )
      expect(job.id).to be_a(Integer)
      expect(job.id).to be > 0
    end

    it 'finds the created job' do
      job = SolidQueue::Job.create!(
        queue_name: 'default',
        class_name: 'FindTestJob',
        arguments: '[]',
        priority: 0
      )
      found = SolidQueue::Job.find(job.id)
      expect(found.class_name).to eq('FindTestJob')
    end

    it 'counts jobs' do
      before_count = SolidQueue::Job.count
      SolidQueue::Job.create!(queue_name: 'default', class_name: 'CountJob', arguments: '[]', priority: 0)
      expect(SolidQueue::Job.count).to eq(before_count + 1)
    end

    it 'destroys a job' do
      job = SolidQueue::Job.create!(queue_name: 'default', class_name: 'DestroyJob', arguments: '[]', priority: 0)
      id = job.id
      job.destroy
      expect(SolidQueue::Job.find_by(id: id)).to be_nil
    end
  end

  # -----------------------------------------------------------------------
  # SolidQueue::Process — Supervisor 起動時に INSERT される箇所
  # -----------------------------------------------------------------------

  describe SolidQueue::Process do
    it 'creates a process record (the INSERT that was failing)' do
      process = SolidQueue::Process.create!(
        kind: 'Worker',
        last_heartbeat_at: Time.now,
        pid: Process.pid,
        hostname: Socket.gethostname,
        name: "worker-#{Process.pid}-#{SecureRandom.hex(4)}"
      )
      expect(process.id).to be_a(Integer)
      expect(process.id).to be > 0
    end

    it 'finds the created process' do
      name = "worker-find-#{Process.pid}-#{SecureRandom.hex(4)}"
      SolidQueue::Process.create!(
        kind: 'Worker',
        last_heartbeat_at: Time.now,
        pid: Process.pid,
        hostname: Socket.gethostname,
        name: name
      )
      found = SolidQueue::Process.find_by(name: name)
      expect(found).not_to be_nil
      expect(found.kind).to eq('Worker')
    end

    it 'destroys a process record' do
      process = SolidQueue::Process.create!(
        kind: 'Supervisor',
        last_heartbeat_at: Time.now,
        pid: Process.pid,
        hostname: Socket.gethostname,
        name: "supervisor-destroy-#{Process.pid}-#{SecureRandom.hex(4)}"
      )
      id = process.id
      process.destroy
      expect(SolidQueue::Process.find_by(id: id)).to be_nil
    end
  end

  # -----------------------------------------------------------------------
  # SolidQueue::ReadyExecution — ジョブのエンキュー
  # -----------------------------------------------------------------------

  describe SolidQueue::ReadyExecution do
    let!(:job) do
      SolidQueue::Job.create!(
        queue_name: 'default',
        class_name: 'EnqueueTestJob',
        arguments: '[1, 2, 3]',
        priority: 0
      )
    end

    it 'enqueues a job as ready execution' do
      execution = SolidQueue::ReadyExecution.create!(
        job_id: job.id,
        queue_name: job.queue_name,
        priority: job.priority
      )
      expect(execution.id).to be_a(Integer)
      expect(execution.job_id).to eq(job.id)
    end

    it 'finds ready executions for a queue' do
      SolidQueue::ReadyExecution.create!(
        job_id: job.id,
        queue_name: 'default',
        priority: 0
      )
      results = SolidQueue::ReadyExecution.where(queue_name: 'default')
      expect(results.count).to be >= 1
    end
  end

  # -----------------------------------------------------------------------
  # SolidQueue::ScheduledExecution — スケジュール実行
  # -----------------------------------------------------------------------

  describe SolidQueue::ScheduledExecution do
    let!(:job) do
      SolidQueue::Job.create!(
        queue_name: 'default',
        class_name: 'ScheduledTestJob',
        arguments: '[]',
        priority: 0,
        scheduled_at: Time.now + 3600
      )
    end

    it 'creates a scheduled execution' do
      execution = SolidQueue::ScheduledExecution.create!(
        job_id: job.id,
        queue_name: job.queue_name,
        priority: job.priority,
        scheduled_at: job.scheduled_at
      )
      expect(execution.id).to be_a(Integer)
    end
  end
end
