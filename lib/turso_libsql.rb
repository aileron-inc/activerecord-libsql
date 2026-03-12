# frozen_string_literal: true

require_relative 'turso_libsql/connection'
require_relative 'turso_libsql/database'

module TursoLibsql
  # fork 後の子プロセスで呼ぶ（Ruby 実装では no-op）
  # Net::HTTP は fork 後も安全なため何もしなくてよい
  def self.reinitialize_runtime!
    # no-op: Ruby の Net::HTTP は fork 後も安全
  end
end
