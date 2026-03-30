# frozen_string_literal: true

require_relative 'lib/activerecord/libsql/version'

Gem::Specification.new do |spec|
  spec.name = 'activerecord-libsql'
  spec.version = ActiveRecord::Libsql::VERSION
  spec.authors = ['aileron']
  spec.email = []

  spec.summary = 'ActiveRecord adapter for Turso (libSQL) database'
  spec.description = <<~DESC
    An ActiveRecord adapter for Turso, the edge SQLite database powered by libSQL.
    Connects to Turso Cloud via the Hrana v2 HTTP protocol using Ruby's built-in
    Net::HTTP, making it fork-safe and dependency-free. Supports Embedded Replica
    mode via the sqlite3 gem for local read performance.
  DESC
  spec.homepage = 'https://github.com/aileron-inc/activerecord-libsql'
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 3.1.0'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage

  spec.files = Dir[
    'lib/**/*.rb',
    'lib/**/*.rake',
    '*.md',
    '*.gemspec'
  ]

  spec.require_paths = ['lib']

  spec.add_dependency 'activerecord', '>= 7.0'
  spec.add_dependency 'sqlite3', '>= 1.4'

  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rake-compiler', '~> 1.2'
  spec.add_development_dependency 'rspec', '~> 3.0'
end
