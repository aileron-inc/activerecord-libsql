# frozen_string_literal: true

require 'net/http'
require 'json'
require 'uri'

module TursoLibsql
  # Hrana v2 HTTP プロトコルを使ったリモート接続
  # Net::HTTP を使用するため fork 後も安全
  class Connection
    def initialize(url, token)
      @hrana_url = hrana_url(url)
      @token = token
      @baton = nil
      @last_insert_rowid = 0
      @last_affected_rows = 0
    end

    # SQL を実行し、影響を受けた行数を返す（INSERT/UPDATE/DELETE 用）
    def execute(sql)
      execute_sql(sql, [])
    end

    # SQL を実行し、結果を Array of Hash で返す（SELECT 用）
    def query(sql)
      query_sql(sql, [])
    end

    # プリペアドステートメントで SQL を実行（パラメータ付き）
    def execute_with_params(sql, params)
      json_params = params.map { |p| { 'type' => 'text', 'value' => p.to_s } }
      execute_sql(sql, json_params)
    end

    # トランザクションを開始する
    def begin_transaction
      requests = [{ 'type' => 'execute', 'stmt' => { 'sql' => 'BEGIN' } }]
      resp = hrana_pipeline(nil, requests)
      @baton = resp['baton']
      check_errors(resp)
    end

    # トランザクションをコミットする
    def commit_transaction
      requests = [
        { 'type' => 'execute', 'stmt' => { 'sql' => 'COMMIT' } },
        { 'type' => 'close' }
      ]
      resp = hrana_pipeline(@baton, requests)
      @baton = nil
      check_errors(resp)
    end

    # トランザクションをロールバックする
    def rollback_transaction
      requests = [
        { 'type' => 'execute', 'stmt' => { 'sql' => 'ROLLBACK' } },
        { 'type' => 'close' }
      ]
      # baton が無効になっている場合（サーバー側でエラー後に破棄された場合）は
      # baton なしで ROLLBACK を試みる。失敗しても無視する（接続は既に破棄済み）
      baton = @baton
      @baton = nil
      begin
        resp = hrana_pipeline(baton, requests)
        check_errors(resp)
      rescue StandardError
        # ROLLBACK 失敗は無視（接続が既に破棄されている場合）
      end
    end

    # 最後に挿入した行の rowid を返す
    attr_reader :last_insert_rowid

    private

    def execute_sql(sql, params)
      stmt = build_stmt(sql, params)
      requests = if @baton
                   [stmt]
                 else
                   [stmt, { 'type' => 'close' }]
                 end

      resp = hrana_pipeline(@baton, requests)
      check_errors(resp)

      @baton = resp['baton'] if @baton
      if (results = resp['results'])&.first
        @last_affected_rows = results.first.dig('response', 'result', 'affected_row_count').to_i
        rowid_str = results.first.dig('response', 'result', 'last_insert_rowid')
        @last_insert_rowid = rowid_str.to_i
      end

      @last_affected_rows
    end

    def query_sql(sql, params)
      stmt = build_stmt(sql, params)
      requests = if @baton
                   [stmt]
                 else
                   [stmt, { 'type' => 'close' }]
                 end

      resp = hrana_pipeline(@baton, requests)
      check_errors(resp)

      @baton = resp['baton'] if @baton

      result = resp.dig('results', 0, 'response', 'result')
      return [] unless result

      cols = result['cols']&.map { |c| c['name'] } || []
      rows = result['rows'] || []

      rows.map do |row|
        record = {}
        cols.each_with_index do |col, i|
          record[col] = hrana_value_to_ruby(row[i])
        end
        record
      end
    end

    def build_stmt(sql, params)
      stmt = { 'sql' => sql }
      stmt['args'] = params if params.any?
      { 'type' => 'execute', 'stmt' => stmt }
    end

    def hrana_pipeline(baton, requests)
      body = { 'requests' => requests }
      body['baton'] = baton if baton

      uri = URI.parse(@hrana_url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = (uri.scheme == 'https')
      http.open_timeout = 10
      http.read_timeout = 30

      request = Net::HTTP::Post.new(uri.path.empty? ? '/' : uri.path)
      request['Authorization'] = "Bearer #{@token}"
      request['Content-Type'] = 'application/json'
      request.body = JSON.generate(body)

      response = http.request(request)

      raise "HTTP error #{response.code}: #{response.body}" unless response.is_a?(Net::HTTPSuccess)

      JSON.parse(response.body)
    rescue Net::OpenTimeout, Net::ReadTimeout, Errno::ECONNREFUSED,
           SocketError, Socket::ResolutionError => e
      raise "HTTP request failed: #{@hrana_url}: #{e.message}"
    end

    def check_errors(resp)
      return unless (results = resp['results'])

      results.each do |r|
        next unless r['type'] == 'error'

        msg = r.dig('error', 'message') || 'Unknown error'
        raise msg
      end
    end

    def hrana_value_to_ruby(cell)
      return nil if cell.nil?

      type = cell['type']
      val  = cell['value']

      case type
      when 'null'    then nil
      when 'integer' then val.to_i
      when 'float'   then val.to_f
      when 'text'    then val.to_s
      when 'blob'    then val.to_s
      end
    end

    def hrana_url(url)
      # libsql:// → https:// に変換
      if url.start_with?('libsql://')
        "https://#{url.sub('libsql://', '')}/v2/pipeline"
      else
        "#{url.chomp('/')}/v2/pipeline"
      end
    end
  end
end
