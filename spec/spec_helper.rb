# frozen_string_literal: true

require 'bundler/setup'
require 'active_record'
require 'active_record/database_configurations'
require 'activerecord-libsql'

# 統合テスト用: .env から環境変数を読み込む
if File.exist?(File.join(__dir__, '..', '.env'))
  File.readlines(File.join(__dir__, '..', '.env')).each do |line|
    line = line.strip
    next if line.empty? || line.start_with?('#')

    key, value = line.split('=', 2)
    ENV[key] ||= value
  end
end

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.disable_monkey_patching!
  config.warnings = true
  config.order = :random
  Kernel.srand config.seed
end
