# frozen_string_literal: true

require_relative 'lib/activerecord/turso/version'

Gem::Specification.new do |spec|
  spec.name = 'activerecord-turso'
  spec.version = ActiveRecord::Turso::VERSION
  spec.authors = ['aileron']
  spec.email = []

  spec.summary = 'ActiveRecord adapter for Turso (libSQL) database'
  spec.description = <<~DESC
    An ActiveRecord adapter for Turso, the edge SQLite database powered by libSQL.
    Uses a native Rust extension (via magnus) to connect directly to Turso via the
    libSQL remote protocol, without requiring any external HTTP client.
  DESC
  spec.homepage = 'https://github.com/aileron/activerecord-turso'
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 3.1.0'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage

  spec.files = Dir[
    'lib/**/*.rb',
    'ext/**/*.{rs,toml,rb}',
    '*.md',
    '*.gemspec',
    'Cargo.{toml,lock}'
  ]

  spec.require_paths = ['lib']
  spec.extensions = ['ext/turso_libsql/extconf.rb']

  spec.add_dependency 'activerecord', '>= 7.0'
  spec.add_dependency 'rb_sys', '~> 0.9'

  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rake-compiler', '~> 1.2'
  spec.add_development_dependency 'rspec', '~> 3.0'
end
