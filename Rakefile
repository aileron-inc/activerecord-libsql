# frozen_string_literal: true

require "rake/extensiontask"
require "rb_sys/extensiontask"
require "rspec/core/rake_task"

GEMSPEC = Gem::Specification.load("activerecord-libsql.gemspec")

RbSys::ExtensionTask.new("turso_libsql", GEMSPEC) do |ext|
  ext.lib_dir = "lib/turso_libsql"
  ext.ext_dir = "ext/turso_libsql"
end

# 単体テストのみ（モックベース、実接続不要）
RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern = "spec/unit/**/*_spec.rb"
  t.rspec_opts = "--format documentation"
end

# 統合テストのみ（実接続が必要）
RSpec::Core::RakeTask.new("spec:integration") do |t|
  t.pattern = "spec/integration/**/*_spec.rb"
  t.rspec_opts = "--format documentation"
end

# 全テスト
RSpec::Core::RakeTask.new("spec:all") do |t|
  t.pattern = "spec/**/*_spec.rb"
  t.rspec_opts = "--format documentation"
end

# turso タスク（standalone 用）
# Rails がない場合は :environment タスクを自前で定義して establish_connection する
unless Rake::Task.task_defined?(:environment)
  task :environment do
    require "dotenv/load" if Gem.find_files("dotenv").any?
    require "activerecord-libsql"
    ActiveRecord::Base.establish_connection(
      adapter:      "turso",
      database:     ENV.fetch("TURSO_DATABASE_URL"),
      token:        ENV.fetch("TURSO_AUTH_TOKEN"),
      replica_path: ENV["TURSO_REPLICA_PATH"]
    )
    # replica ファイルを初期化するために接続を確立する
    ActiveRecord::Base.connection.execute("SELECT 1")
  end
end

load File.expand_path("lib/tasks/turso.rake", __dir__)

task default: :compile
