# frozen_string_literal: true

require 'spec_helper'

# 実接続テスト: TURSO_DATABASE_URL と TURSO_AUTH_TOKEN が必要
RSpec.describe 'ActiveRecord integration', :integration do
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
  end

  # テスト用テーブル名（並列実行時の衝突を避けるため PID を付与）
  let(:table_name) { "spec_users_#{Process.pid}" }

  # テスト用 AR モデル（テーブル名を動的に設定）
  let(:model_class) do
    klass = Class.new(ActiveRecord::Base) do
      self.primary_key = 'id'
    end
    klass.table_name = table_name
    klass
  end

  before do
    ActiveRecord::Base.connection.create_table(table_name, force: true) do |t|
      t.string  :name, null: false
      t.string  :email
      t.integer :age
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(table_name, if_exists: true)
  end

  # -----------------------------------------------------------------------
  # スキーマ情報
  # -----------------------------------------------------------------------

  describe 'schema introspection' do
    it 'lists the test table' do
      expect(ActiveRecord::Base.connection.tables).to include(table_name)
    end

    it 'returns column definitions' do
      columns = ActiveRecord::Base.connection.columns(table_name)
      names = columns.map(&:name)
      expect(names).to include('id', 'name', 'email', 'age')
    end

    it 'recognizes table_exists?' do
      expect(ActiveRecord::Base.connection.table_exists?(table_name)).to be true
      expect(ActiveRecord::Base.connection.table_exists?('nonexistent_table')).to be false
    end
  end

  # -----------------------------------------------------------------------
  # CRUD
  # -----------------------------------------------------------------------

  describe 'CREATE (INSERT)' do
    it 'creates a record and returns it with an id' do
      record = model_class.create!(name: 'Alice', email: 'alice@example.com', age: 30)
      expect(record.id).to be_a(Integer)
      expect(record.id).to be > 0
      expect(record.name).to eq('Alice')
    end

    it 'raises on NOT NULL violation' do
      expect do
        model_class.create!(email: 'no-name@example.com')
      end.to raise_error(ActiveRecord::NotNullViolation)
    end
  end

  describe 'READ (SELECT)' do
    before do
      model_class.create!(name: 'Alice', email: 'alice@example.com', age: 30)
      model_class.create!(name: 'Bob',   email: 'bob@example.com',   age: 25)
    end

    it 'finds all records' do
      expect(model_class.all.count).to eq(2)
    end

    it 'finds by id' do
      alice = model_class.find_by(name: 'Alice')
      found = model_class.find(alice.id)
      expect(found.name).to eq('Alice')
    end

    it 'filters with where' do
      results = model_class.where(name: 'Bob')
      expect(results.count).to eq(1)
      expect(results.first.email).to eq('bob@example.com')
    end

    it 'returns nil for find_by with no match' do
      expect(model_class.find_by(name: 'Nobody')).to be_nil
    end

    it 'orders records' do
      names = model_class.order(:name).pluck(:name)
      expect(names).to eq(%w[Alice Bob])
    end

    it 'limits records' do
      expect(model_class.limit(1).count).to eq(1)
    end
  end

  describe 'UPDATE' do
    let!(:record) { model_class.create!(name: 'Alice', email: 'alice@example.com', age: 30) }

    it 'updates a single attribute' do
      record.update!(email: 'new@example.com')
      reloaded = model_class.find(record.id)
      expect(reloaded.email).to eq('new@example.com')
    end

    it 'updates multiple attributes' do
      record.update!(name: 'Alicia', age: 31)
      reloaded = model_class.find(record.id)
      expect(reloaded.name).to eq('Alicia')
      expect(reloaded.age).to eq(31)
    end
  end

  describe 'DELETE' do
    let!(:record) { model_class.create!(name: 'Alice', email: 'alice@example.com', age: 30) }

    it 'destroys a record' do
      id = record.id
      record.destroy
      expect(model_class.find_by(id: id)).to be_nil
    end

    it 'deletes all records' do
      model_class.create!(name: 'Bob', email: 'bob@example.com', age: 25)
      model_class.delete_all
      expect(model_class.count).to eq(0)
    end
  end

  # -----------------------------------------------------------------------
  # トランザクション
  # -----------------------------------------------------------------------

  describe 'transactions' do
    it 'commits on success' do
      ActiveRecord::Base.transaction do
        model_class.create!(name: 'Committed', email: 'c@example.com', age: 20)
      end
      expect(model_class.find_by(name: 'Committed')).not_to be_nil
    end

    it 'rolls back on exception' do
      expect do
        ActiveRecord::Base.transaction do
          model_class.create!(name: 'RolledBack', email: 'r@example.com', age: 20)
          raise ActiveRecord::Rollback
        end
      end.not_to raise_error

      expect(model_class.find_by(name: 'RolledBack')).to be_nil
    end
  end
end
