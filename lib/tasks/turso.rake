# frozen_string_literal: true

require 'shellwords'
require 'tempfile'
require 'fileutils'

namespace :turso do
  namespace :schema do
    desc <<~DESC
      Apply schema to Turso Cloud using sqldef (sqlite3def).

      Compares the desired schema (schema.sql) against the current remote schema
      and applies only the diff — idempotent and declarative.

      Usage:
        rake turso:schema:apply[db/schema.sql]

      Prerequisites:
        - sqlite3def must be installed (https://github.com/sqldef/sqldef)
        - database.yml must have replica_path configured

      Flow:
        1. Pull latest frames from remote into local replica
        2. Copy replica to a temp file for sqlite3def (avoids libsql metadata conflict)
        3. Run sqlite3def --dry-run to compute diff SQL
        4. If no diff, exit normally ("Already up to date")
        5. Apply diff SQL to Turso Cloud
        6. Pull again to confirm
    DESC
    task :apply, [:schema_file] => :environment do |_t, args|
      schema_file = args[:schema_file]
      abort 'Usage: rake turso:schema:apply[path/to/schema.sql]' unless schema_file
      abort "Schema file not found: #{schema_file}" unless File.exist?(schema_file)

      unless system('which sqlite3def > /dev/null 2>&1')
        abort 'sqlite3def not found. Install it from https://github.com/sqldef/sqldef'
      end

      conn = ActiveRecord::Base.connection
      replica_path = conn.instance_variable_get(:@config)&.dig(:replica_path)
      abort 'replica_path is not configured in database.yml' unless replica_path
      abort "Replica file not found: #{replica_path} (run the app first to initialize)" \
        unless File.exist?(replica_path)

      puts '==> [1/4] Pulling latest schema from remote...'
      conn.sync
      puts '    Done.'

      # sqlite3def は SQLite ファイルを直接開くため、libsql の replica と競合しないよう
      # 一時ファイルにコピーして使う。WAL モードの場合は -wal / -shm も一緒にコピーする。
      tmp_db = Tempfile.new(['turso_schema_diff', '.db'])
      tmp_db.close
      begin
        FileUtils.cp(replica_path, tmp_db.path)
        %w[-wal -shm].each do |suffix|
          src = "#{replica_path}#{suffix}"
          FileUtils.cp(src, "#{tmp_db.path}#{suffix}") if File.exist?(src)
        end

        puts '==> [2/4] Computing schema diff...'
        diff_sql = `sqlite3def --dry-run --file #{Shellwords.escape(schema_file)} #{Shellwords.escape(tmp_db.path)} 2>&1`
        exit_status = $?.exitstatus

        abort "sqlite3def failed (exit #{exit_status}):\n#{diff_sql}" if exit_status != 0
      ensure
        tmp_db.unlink
        FileUtils.rm_f("#{tmp_db.path}-wal")
        FileUtils.rm_f("#{tmp_db.path}-shm")
      end

      # BEGIN / COMMIT / コメント行（--）を除外する
      statements = diff_sql
                   .split(';')
                   .map(&:strip)
                   .reject(&:empty?)
                   .reject { |s| s.match?(/\A(BEGIN|COMMIT|ROLLBACK)\z/i) }
                   .reject { |s| s.match?(/\A--/) }

      if statements.empty?
        puts '    Already up to date.'
        next
      end

      puts "    #{statements.size} statement(s) to apply:"
      statements.each { |s| puts "      #{s.lines.first&.strip}" }

      puts '==> [3/4] Applying schema to Turso Cloud...'
      # libsql は DDL トランザクションをサポートしないため、直接実行する
      statements.each { |sql| conn.execute(sql) }
      puts '    Done.'

      puts '==> [4/4] Pulling to confirm...'
      conn.sync
      puts '    Done.'

      puts '==> Schema applied successfully!'
    end

    desc 'Show schema diff between schema.sql and Turso Cloud (dry-run, no changes applied)'
    task :diff, [:schema_file] => :environment do |_t, args|
      schema_file = args[:schema_file]
      abort 'Usage: rake turso:schema:diff[path/to/schema.sql]' unless schema_file
      abort "Schema file not found: #{schema_file}" unless File.exist?(schema_file)

      unless system('which sqlite3def > /dev/null 2>&1')
        abort 'sqlite3def not found. Install it from https://github.com/sqldef/sqldef'
      end

      conn = ActiveRecord::Base.connection
      replica_path = conn.instance_variable_get(:@config)&.dig(:replica_path)
      abort 'replica_path is not configured in database.yml' unless replica_path
      abort "Replica file not found: #{replica_path}" unless File.exist?(replica_path)

      puts '==> Pulling latest schema from remote...'
      conn.sync

      tmp_db = Tempfile.new(['turso_schema_diff', '.db'])
      tmp_db.close
      begin
        FileUtils.cp(replica_path, tmp_db.path)
        %w[-wal -shm].each do |suffix|
          src = "#{replica_path}#{suffix}"
          FileUtils.cp(src, "#{tmp_db.path}#{suffix}") if File.exist?(src)
        end

        puts '==> Schema diff (sqlite3def dry-run):'
        diff_sql = `sqlite3def --dry-run --file #{Shellwords.escape(schema_file)} #{Shellwords.escape(tmp_db.path)} 2>&1`

        statements = diff_sql
                     .split(';')
                     .map(&:strip)
                     .reject(&:empty?)
                     .reject { |s| s.match?(/\A(BEGIN|COMMIT|ROLLBACK)\z/i) }
                     .reject { |s| s.match?(/\A--/) }

        if statements.empty?
          puts '    No changes. Already up to date.'
        else
          puts diff_sql
        end
      ensure
        tmp_db.unlink
        FileUtils.rm_f("#{tmp_db.path}-wal")
        FileUtils.rm_f("#{tmp_db.path}-shm")
      end
    end
  end
end
