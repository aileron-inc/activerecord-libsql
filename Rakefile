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

task default: :compile
