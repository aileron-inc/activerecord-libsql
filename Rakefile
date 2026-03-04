# frozen_string_literal: true

require "rake/extensiontask"
require "rb_sys/extensiontask"

GEMSPEC = Gem::Specification.load("activerecord-libsql.gemspec")

RbSys::ExtensionTask.new("turso_libsql", GEMSPEC) do |ext|
  ext.lib_dir = "lib/turso_libsql"
  ext.ext_dir = "ext/turso_libsql"
end

task default: :compile
