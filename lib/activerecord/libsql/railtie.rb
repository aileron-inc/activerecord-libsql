# frozen_string_literal: true

require 'rails/railtie'

module ActiveRecord
  module Libsql
    class Railtie < Rails::Railtie
      rake_tasks do
        load File.expand_path('../../tasks/turso.rake', __dir__)
      end
    end
  end
end
