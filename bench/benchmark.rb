# frozen_string_literal: true

# Benchmark script for activerecord-libsql
# Usage: bundle exec ruby bench/benchmark.rb
#
# Requires: TURSO_DATABASE_URL and TURSO_AUTH_TOKEN env vars

require 'bundler/setup'
require 'benchmark'
require 'activerecord-libsql'

# Load .env
env_file = File.join(__dir__, '..', '.env')
if File.exist?(env_file)
  File.readlines(env_file).each do |line|
    line = line.strip
    next if line.empty? || line.start_with?('#')

    key, value = line.split('=', 2)
    ENV[key] ||= value
  end
end

abort 'Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN' unless ENV['TURSO_DATABASE_URL'] && ENV['TURSO_AUTH_TOKEN']

# -----------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------

ActiveRecord::Base.establish_connection(
  adapter: 'turso',
  database: ENV['TURSO_DATABASE_URL'],
  token: ENV['TURSO_AUTH_TOKEN']
)

conn = ActiveRecord::Base.connection

TABLE = '_bench_users'

conn.drop_table(TABLE, if_exists: true)
conn.create_table(TABLE) do |t|
  t.string  :name, null: false
  t.string  :email
  t.integer :age
end

class BenchUser < ActiveRecord::Base
  self.table_name = TABLE
end

ITERATIONS = 100

puts '=' * 60
puts 'activerecord-libsql Benchmark'
puts 'Turso remote database (libSQL over HTTPS)'
puts "Iterations: #{ITERATIONS} per operation"
puts '=' * 60
puts

# -----------------------------------------------------------------------
# Warmup
# -----------------------------------------------------------------------

print 'Warming up... '
5.times { BenchUser.create!(name: 'warmup', email: 'w@example.com', age: 0) }
BenchUser.delete_all
puts "done\n\n"

# -----------------------------------------------------------------------
# Benchmarks
# -----------------------------------------------------------------------

results = {}

Benchmark.bm(30) do |x|
  # INSERT
  results[:insert] = x.report('INSERT (single row):') do
    ITERATIONS.times do |i|
      BenchUser.create!(name: "User#{i}", email: "user#{i}@example.com", age: i % 80)
    end
  end

  total_rows = BenchUser.count
  puts "  → #{total_rows} rows in table after INSERT benchmark"

  # SELECT all
  results[:select_all] = x.report("SELECT all (#{total_rows} rows):") do
    ITERATIONS.times { BenchUser.all.to_a }
  end

  # SELECT with WHERE
  results[:select_where] = x.report('SELECT WHERE (indexed):') do
    ITERATIONS.times { |i| BenchUser.where(name: "User#{i % ITERATIONS}").to_a }
  end

  # SELECT single by id
  ids = BenchUser.limit(ITERATIONS).pluck(:id)
  results[:select_find] = x.report('SELECT find by id:') do
    ids.each { |id| BenchUser.find(id) }
  end

  # UPDATE
  sample_ids = BenchUser.limit(ITERATIONS).pluck(:id)
  results[:update] = x.report('UPDATE (single row):') do
    sample_ids.each { |id| BenchUser.find(id).update!(age: rand(80)) }
  end

  # DELETE
  results[:delete] = x.report('DELETE (single row):') do
    sample_ids.each { |id| BenchUser.find(id).destroy }
  end

  # Transaction (batch insert)
  BenchUser.delete_all
  results[:transaction] = x.report('Transaction (10 inserts):') do
    ITERATIONS.times do |i|
      ActiveRecord::Base.transaction do
        10.times do |j|
          BenchUser.create!(name: "TxUser#{i}_#{j}", email: "tx#{i}_#{j}@example.com", age: j)
        end
      end
    end
  end
end

# -----------------------------------------------------------------------
# Summary (ops/sec)
# -----------------------------------------------------------------------

puts
puts '=' * 60
puts 'Operations per second (higher is better)'
puts '=' * 60

ops_map = {
  insert: ['INSERT single row', ITERATIONS],
  select_all: ['SELECT all rows', ITERATIONS],
  select_where: ['SELECT WHERE', ITERATIONS],
  select_find: ['SELECT find by id', ITERATIONS],
  update: ['UPDATE single row',       ITERATIONS],
  delete: ['DELETE single row',       ITERATIONS],
  transaction: ['Transaction (10 inserts)', ITERATIONS]
}

ops_map.each do |key, (label, n)|
  t = results[key].real
  ops = (n / t).round(1)
  avg_ms = ((t / n) * 1000).round(1)
  printf "  %-30s %7.1f ops/s  (avg %5.1f ms/op)\n", label, ops, avg_ms
end

puts

# Cleanup
conn.drop_table(TABLE, if_exists: true)
puts 'Cleanup done.'
