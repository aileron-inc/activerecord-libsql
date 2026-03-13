use curl::easy::{Easy, List};
use magnus::{function, method, prelude::*, Error, Ruby};
use serde_json::{json, Value as JsonValue};
use std::sync::{Arc, Mutex};

// -----------------------------------------------------------------------
// Hrana v2 HTTP クライアント
// -----------------------------------------------------------------------
//
// Turso の HTTP API: POST /v2/pipeline
// tokio 不要・同期 HTTP → fork 後も安全
//
// トランザクション管理:
//   baton = None  → autocommit（1 リクエストで完結）
//   baton = Some  → ステートフル接続（BEGIN〜COMMIT/ROLLBACK）
// -----------------------------------------------------------------------

fn hrana_url(database_url: &str) -> String {
    // libsql:// → https:// に変換
    if let Some(rest) = database_url.strip_prefix("libsql://") {
        format!("https://{}/v2/pipeline", rest)
    } else {
        format!("{}/v2/pipeline", database_url.trim_end_matches('/'))
    }
}

fn mk_err(msg: impl std::fmt::Display) -> Error {
    let ruby = Ruby::get().expect("called outside Ruby thread");
    Error::new(ruby.exception_runtime_error(), msg.to_string())
}

/// Hrana v2 pipeline リクエストを送信する
/// baton: None = autocommit, Some(s) = ステートフル接続
/// curl を使用（libcurl は fork 後も安全）
fn hrana_pipeline(
    url: &str,
    token: &str,
    baton: Option<&str>,
    requests: &[JsonValue],
) -> Result<JsonValue, Error> {
    let mut body = json!({ "requests": requests });
    if let Some(b) = baton {
        body["baton"] = json!(b);
    }

    let body_str = serde_json::to_string(&body)
        .map_err(|e| mk_err(format!("Failed to serialize request: {}", e)))?;

    let mut easy = Easy::new();
    easy.url(url)
        .map_err(|e| mk_err(format!("curl URL error: {}", e)))?;
    easy.post(true)
        .map_err(|e| mk_err(format!("curl POST error: {}", e)))?;
    easy.post_field_size(body_str.len() as u64)
        .map_err(|e| mk_err(format!("curl post_field_size error: {}", e)))?;

    let mut headers = List::new();
    headers
        .append(&format!("Authorization: Bearer {}", token))
        .map_err(|e| mk_err(format!("curl header error: {}", e)))?;
    headers
        .append("Content-Type: application/json")
        .map_err(|e| mk_err(format!("curl header error: {}", e)))?;
    easy.http_headers(headers)
        .map_err(|e| mk_err(format!("curl http_headers error: {}", e)))?;

    let body_bytes = body_str.into_bytes();
    let mut body_cursor = std::io::Cursor::new(body_bytes);
    let mut response_bytes = Vec::new();

    {
        let mut transfer = easy.transfer();
        transfer
            .read_function(|buf| {
                use std::io::Read;
                Ok(body_cursor.read(buf).unwrap_or(0))
            })
            .map_err(|e| mk_err(format!("curl read_function error: {}", e)))?;
        transfer
            .write_function(|data| {
                response_bytes.extend_from_slice(data);
                Ok(data.len())
            })
            .map_err(|e| mk_err(format!("curl write_function error: {}", e)))?;
        transfer
            .perform()
            .map_err(|e| mk_err(format!("HTTP request failed: {}", e)))?;
    }

    let response_code = easy
        .response_code()
        .map_err(|e| mk_err(format!("curl response_code error: {}", e)))?;

    if response_code < 200 || response_code >= 300 {
        let body = String::from_utf8_lossy(&response_bytes);
        return Err(mk_err(format!(
            "HTTP error {}: {}",
            response_code, body
        )));
    }

    let json: JsonValue = serde_json::from_slice(&response_bytes)
        .map_err(|e| mk_err(format!("Failed to parse response: {}", e)))?;

    Ok(json)
}

/// Hrana レスポンスから rows を取り出す（SELECT 用）
fn extract_rows(result: &JsonValue) -> Result<Vec<Vec<(String, JsonValue)>>, Error> {
    let response = result
        .get("response")
        .and_then(|r| r.get("result"))
        .ok_or_else(|| mk_err("Invalid response structure"))?;

    let cols = response
        .get("cols")
        .and_then(|c| c.as_array())
        .ok_or_else(|| mk_err("Missing cols in response"))?;

    let rows = response
        .get("rows")
        .and_then(|r| r.as_array())
        .ok_or_else(|| mk_err("Missing rows in response"))?;

    let col_names: Vec<String> = cols
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("?")
                .to_string()
        })
        .collect();

    let mut result_rows = Vec::new();
    for row in rows {
        let cells = row.as_array().ok_or_else(|| mk_err("Row is not an array"))?;
        let mut record = Vec::new();
        for (i, cell) in cells.iter().enumerate() {
            let col_name = col_names.get(i).cloned().unwrap_or_else(|| "?".to_string());
            record.push((col_name, cell.clone()));
        }
        result_rows.push(record);
    }

    Ok(result_rows)
}

/// Hrana レスポンスから affected_row_count を取り出す（INSERT/UPDATE/DELETE 用）
fn extract_affected_rows(result: &JsonValue) -> u64 {
    result
        .get("response")
        .and_then(|r| r.get("result"))
        .and_then(|r| r.get("affected_row_count"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0)
}

/// Hrana レスポンスから last_insert_rowid を取り出す
fn extract_last_insert_rowid(result: &JsonValue) -> i64 {
    result
        .get("response")
        .and_then(|r| r.get("result"))
        .and_then(|r| r.get("last_insert_rowid"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Hrana Value → Ruby Value 変換
fn hrana_value_to_ruby(ruby: &Ruby, cell: &JsonValue) -> Result<magnus::Value, Error> {
    // Hrana の値形式: {"type": "integer", "value": "42"} または null
    if cell.is_null() {
        return Ok(ruby.qnil().as_value());
    }

    let typ = cell.get("type").and_then(|t| t.as_str()).unwrap_or("null");
    let val = cell.get("value");

    match typ {
        "null" => Ok(ruby.qnil().as_value()),
        "integer" => {
            let n = val
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            Ok(ruby.integer_from_i64(n).as_value())
        }
        "float" => {
            let f = val
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            Ok(ruby.float_from_f64(f).as_value())
        }
        "text" => {
            let s = val.and_then(|v| v.as_str()).unwrap_or("");
            Ok(ruby.str_new(s).as_value())
        }
        "blob" => {
            // base64 エンコードされたバイナリ
            let s = val.and_then(|v| v.as_str()).unwrap_or("");
            Ok(ruby.str_new(s).as_value())
        }
        _ => Ok(ruby.qnil().as_value()),
    }
}

// -----------------------------------------------------------------------
// TursoConnection — リモート接続（Hrana HTTP）
// -----------------------------------------------------------------------

struct RemoteConnection {
    url: String,
    token: String,
    baton: Option<String>,
    last_insert_rowid: i64,
    last_affected_rows: u64,
}

impl RemoteConnection {
    fn new(url: String, token: String) -> Self {
        Self {
            url,
            token,
            baton: None,
            last_insert_rowid: 0,
            last_affected_rows: 0,
        }
    }

    fn execute_sql(&mut self, sql: &str, params: Vec<JsonValue>) -> Result<u64, Error> {
        let stmt = if params.is_empty() {
            json!({ "type": "execute", "stmt": { "sql": sql } })
        } else {
            json!({ "type": "execute", "stmt": { "sql": sql, "args": params } })
        };

        let requests = if self.baton.is_some() {
            vec![stmt]
        } else {
            vec![stmt, json!({ "type": "close" })]
        };

        let resp = hrana_pipeline(
            &self.url,
            &self.token,
            self.baton.as_deref(),
            &requests,
        )?;

        // エラーチェック
        if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
            for r in results {
                if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                    let msg = r
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    return Err(mk_err(msg));
                }
            }

            // baton を更新
            if let Some(new_baton) = resp.get("baton").and_then(|b| b.as_str()) {
                self.baton = Some(new_baton.to_string());
            }

            // 最初の execute 結果から affected_rows と last_insert_rowid を取得
            if let Some(first) = results.first() {
                self.last_affected_rows = extract_affected_rows(first);
                self.last_insert_rowid = extract_last_insert_rowid(first);
            }
        }

        Ok(self.last_affected_rows)
    }

    fn query_sql(
        &mut self,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Result<Vec<Vec<(String, JsonValue)>>, Error> {
        let stmt = if params.is_empty() {
            json!({ "type": "execute", "stmt": { "sql": sql } })
        } else {
            json!({ "type": "execute", "stmt": { "sql": sql, "args": params } })
        };

        let requests = if self.baton.is_some() {
            vec![stmt]
        } else {
            vec![stmt, json!({ "type": "close" })]
        };

        let resp = hrana_pipeline(
            &self.url,
            &self.token,
            self.baton.as_deref(),
            &requests,
        )?;

        // エラーチェック
        if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
            for r in results {
                if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                    let msg = r
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    return Err(mk_err(msg));
                }
            }

            // baton を更新
            if let Some(new_baton) = resp.get("baton").and_then(|b| b.as_str()) {
                self.baton = Some(new_baton.to_string());
            }

            if let Some(first) = results.first() {
                return extract_rows(first);
            }
        }

        Ok(vec![])
    }
}

// -----------------------------------------------------------------------
// LocalConnection — ローカル SQLite 接続（rusqlite）
// -----------------------------------------------------------------------

struct LocalConnection {
    conn: rusqlite::Connection,
    last_insert_rowid: i64,
    last_affected_rows: u64,
}

impl LocalConnection {
    fn open(path: &str) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| mk_err(format!("Failed to open local DB: {}", e)))?;
        Ok(Self {
            conn,
            last_insert_rowid: 0,
            last_affected_rows: 0,
        })
    }

    fn execute_sql(&mut self, sql: &str, params: Vec<String>) -> Result<u64, Error> {
        let params_refs: Vec<&dyn rusqlite::ToSql> = params
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let affected = self
            .conn
            .execute(sql, params_refs.as_slice())
            .map_err(|e| mk_err(format!("SQLite execute error: {}", e)))?;

        self.last_affected_rows = affected as u64;
        self.last_insert_rowid = self.conn.last_insert_rowid();
        Ok(self.last_affected_rows)
    }

    fn query_sql(
        &mut self,
        sql: &str,
        params: Vec<String>,
    ) -> Result<Vec<Vec<(String, rusqlite::types::Value)>>, Error> {
        let params_refs: Vec<&dyn rusqlite::ToSql> = params
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| mk_err(format!("SQLite prepare error: {}", e)))?;

        let col_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut record = Vec::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i)?;
                    record.push((name.clone(), val));
                }
                Ok(record)
            })
            .map_err(|e| mk_err(format!("SQLite query error: {}", e)))?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(|e| mk_err(format!("SQLite row error: {}", e)))?);
        }
        Ok(result)
    }
}

// -----------------------------------------------------------------------
// ConnectionInner — remote / local の統合
// -----------------------------------------------------------------------

enum ConnectionInner {
    Remote(RemoteConnection),
    Local(LocalConnection),
}

// -----------------------------------------------------------------------
// TursoConnection — Ruby に公開する接続オブジェクト
// -----------------------------------------------------------------------

#[magnus::wrap(class = "TursoLibsql::Connection", free_immediately, size)]
struct TursoConnection {
    inner: Arc<Mutex<ConnectionInner>>,
}

impl TursoConnection {
    /// 新しいリモート接続を作成する（Ruby: TursoLibsql::Connection.new(url, token)）
    fn new(url: String, token: String) -> Result<Self, Error> {
        let hrana = hrana_url(&url);
        Ok(Self {
            inner: Arc::new(Mutex::new(ConnectionInner::Remote(RemoteConnection::new(
                hrana, token,
            )))),
        })
    }

    /// SQL を実行し、影響を受けた行数を返す（INSERT/UPDATE/DELETE 用）
    fn execute(&self, sql: String) -> Result<u64, Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;
        match &mut *guard {
            ConnectionInner::Remote(c) => c.execute_sql(&sql, vec![]),
            ConnectionInner::Local(c) => c.execute_sql(&sql, vec![]),
        }
    }

    /// SQL を実行し、結果を Array of Hash で返す（SELECT 用）
    fn query(&self, sql: String) -> Result<magnus::RArray, Error> {
        let ruby = Ruby::get().expect("called outside Ruby thread");
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &mut *guard {
            ConnectionInner::Remote(c) => {
                let rows = c.query_sql(&sql, vec![])?;
                let outer = ruby.ary_new_capa(rows.len());
                for record in rows {
                    let hash = ruby.hash_new();
                    for (col, val) in record {
                        let ruby_key = ruby.str_new(&col);
                        let ruby_val = hrana_value_to_ruby(&ruby, &val)?;
                        hash.aset(ruby_key, ruby_val)?;
                    }
                    outer.push(hash)?;
                }
                Ok(outer)
            }
            ConnectionInner::Local(c) => {
                let rows = c.query_sql(&sql, vec![])?;
                let outer = ruby.ary_new_capa(rows.len());
                for record in rows {
                    let hash = ruby.hash_new();
                    for (col, val) in record {
                        let ruby_key = ruby.str_new(&col);
                        let ruby_val = rusqlite_value_to_ruby(&ruby, val)?;
                        hash.aset(ruby_key, ruby_val)?;
                    }
                    outer.push(hash)?;
                }
                Ok(outer)
            }
        }
    }

    /// プリペアドステートメントで SQL を実行（パラメータ付き）
    fn execute_with_params(&self, sql: String, params: Vec<String>) -> Result<u64, Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &mut *guard {
            ConnectionInner::Remote(c) => {
                let json_params: Vec<JsonValue> = params
                    .into_iter()
                    .map(|s| json!({ "type": "text", "value": s }))
                    .collect();
                c.execute_sql(&sql, json_params)
            }
            ConnectionInner::Local(c) => c.execute_sql(&sql, params),
        }
    }

    /// トランザクションを開始する
    fn begin_transaction(&self) -> Result<(), Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &mut *guard {
            ConnectionInner::Remote(c) => {
                // BEGIN を送って baton を取得する
                let requests = vec![
                    json!({ "type": "execute", "stmt": { "sql": "BEGIN" } }),
                ];
                let resp = hrana_pipeline(&c.url, &c.token, None, &requests)?;

                if let Some(new_baton) = resp.get("baton").and_then(|b| b.as_str()) {
                    c.baton = Some(new_baton.to_string());
                }

                // エラーチェック
                if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
                    for r in results {
                        if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                            let msg = r
                                .get("error")
                                .and_then(|e| e.get("message"))
                                .and_then(|m| m.as_str())
                                .unwrap_or("BEGIN failed");
                            return Err(mk_err(msg));
                        }
                    }
                }
                Ok(())
            }
            ConnectionInner::Local(c) => {
                c.conn
                    .execute_batch("BEGIN")
                    .map_err(|e| mk_err(format!("BEGIN failed: {}", e)))
            }
        }
    }

    /// トランザクションをコミットする
    fn commit_transaction(&self) -> Result<(), Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &mut *guard {
            ConnectionInner::Remote(c) => {
                let requests = vec![
                    json!({ "type": "execute", "stmt": { "sql": "COMMIT" } }),
                    json!({ "type": "close" }),
                ];
                let resp =
                    hrana_pipeline(&c.url, &c.token, c.baton.as_deref(), &requests)?;
                c.baton = None;

                if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
                    for r in results {
                        if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                            let msg = r
                                .get("error")
                                .and_then(|e| e.get("message"))
                                .and_then(|m| m.as_str())
                                .unwrap_or("COMMIT failed");
                            return Err(mk_err(msg));
                        }
                    }
                }
                Ok(())
            }
            ConnectionInner::Local(c) => {
                c.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| mk_err(format!("COMMIT failed: {}", e)))
            }
        }
    }

    /// トランザクションをロールバックする
    fn rollback_transaction(&self) -> Result<(), Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &mut *guard {
            ConnectionInner::Remote(c) => {
                let requests = vec![
                    json!({ "type": "execute", "stmt": { "sql": "ROLLBACK" } }),
                    json!({ "type": "close" }),
                ];
                let resp =
                    hrana_pipeline(&c.url, &c.token, c.baton.as_deref(), &requests)?;
                c.baton = None;

                if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
                    for r in results {
                        if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                            let msg = r
                                .get("error")
                                .and_then(|e| e.get("message"))
                                .and_then(|m| m.as_str())
                                .unwrap_or("ROLLBACK failed");
                            return Err(mk_err(msg));
                        }
                    }
                }
                Ok(())
            }
            ConnectionInner::Local(c) => {
                c.conn
                    .execute_batch("ROLLBACK")
                    .map_err(|e| mk_err(format!("ROLLBACK failed: {}", e)))
            }
        }
    }

    /// 最後に挿入した行の rowid を返す
    fn last_insert_rowid(&self) -> Result<i64, Error> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| mk_err(format!("Lock error: {}", e)))?;

        match &*guard {
            ConnectionInner::Remote(c) => Ok(c.last_insert_rowid),
            ConnectionInner::Local(c) => Ok(c.last_insert_rowid),
        }
    }
}

// -----------------------------------------------------------------------
// rusqlite::Value → Ruby Value 変換
// -----------------------------------------------------------------------

fn rusqlite_value_to_ruby(ruby: &Ruby, val: rusqlite::types::Value) -> Result<magnus::Value, Error> {
    match val {
        rusqlite::types::Value::Null => Ok(ruby.qnil().as_value()),
        rusqlite::types::Value::Integer(i) => Ok(ruby.integer_from_i64(i).as_value()),
        rusqlite::types::Value::Real(f) => Ok(ruby.float_from_f64(f).as_value()),
        rusqlite::types::Value::Text(s) => Ok(ruby.str_new(&s).as_value()),
        rusqlite::types::Value::Blob(b) => Ok(ruby.str_from_slice(&b).as_value()),
    }
}

// -----------------------------------------------------------------------
// TursoDatabase — Database を保持するラッパー
// -----------------------------------------------------------------------

enum DatabaseInner {
    /// リモートのみ（Hrana HTTP）
    Remote { url: String, token: String },
    /// Embedded Replica（ローカル SQLite + リモート同期）
    Replica {
        path: String,
        remote_url: String,
        token: String,
        offline: bool,
    },
}

#[magnus::wrap(class = "TursoLibsql::Database", free_immediately, size)]
struct TursoDatabase {
    inner: Arc<DatabaseInner>,
}

impl TursoDatabase {
    /// リモート接続用 Database を作成
    fn new_remote(url: String, token: String) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(DatabaseInner::Remote { url, token }),
        })
    }

    /// Embedded Replica 用 Database を作成
    fn new_remote_replica(
        path: String,
        url: String,
        token: String,
        _sync_interval_secs: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(DatabaseInner::Replica {
                path,
                remote_url: url,
                token,
                offline: false,
            }),
        })
    }

    /// Offline write 用 Database を作成
    fn new_synced(
        path: String,
        url: String,
        token: String,
        _sync_interval_secs: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(DatabaseInner::Replica {
                path,
                remote_url: url,
                token,
                offline: true,
            }),
        })
    }

    /// リモートから最新フレームを手動で同期する
    fn sync(&self) -> Result<(), Error> {
        match self.inner.as_ref() {
            DatabaseInner::Remote { .. } => {
                // remote モードでは no-op
                Ok(())
            }
            DatabaseInner::Replica {
                path,
                remote_url,
                token,
                offline,
            } => {
                replica_sync(path, remote_url, token, *offline)
            }
        }
    }

    /// この Database から Connection を取得して TursoConnection を返す
    fn connect(&self) -> Result<TursoConnection, Error> {
        match self.inner.as_ref() {
            DatabaseInner::Remote { url, token } => {
                let hrana = hrana_url(url);
                Ok(TursoConnection {
                    inner: Arc::new(Mutex::new(ConnectionInner::Remote(
                        RemoteConnection::new(hrana, token.clone()),
                    ))),
                })
            }
            DatabaseInner::Replica { path, .. } => {
                // ローカルファイルを開く（なければ作成される）
                let local = LocalConnection::open(path)?;
                Ok(TursoConnection {
                    inner: Arc::new(Mutex::new(ConnectionInner::Local(local))),
                })
            }
        }
    }
}

// -----------------------------------------------------------------------
// Embedded Replica 同期ロジック
// -----------------------------------------------------------------------
//
// Hrana HTTP API を使って remote から WAL フレームを取得し、
// ローカル SQLite ファイルに適用する簡易実装。
//
// 実装方針:
//   pull: remote の全テーブルを SELECT して local に UPSERT
//   push (offline): local の変更を remote に INSERT OR REPLACE
//
// 注意: これは完全な WAL レプリケーションではなく、
//       テーブルデータのコピーによる簡易同期。
//       スキーマ変更は pull 時に remote から取得して適用する。
// -----------------------------------------------------------------------

fn replica_sync(path: &str, remote_url: &str, token: &str, offline: bool) -> Result<(), Error> {
    let hrana = hrana_url(remote_url);

    // ローカル DB を開く
    let local = rusqlite::Connection::open(path)
        .map_err(|e| mk_err(format!("Failed to open local DB for sync: {}", e)))?;

    // remote からテーブル一覧を取得
    let tables = remote_get_tables(&hrana, token)?;

    for table in &tables {
        // remote からスキーマを取得
        let schema = remote_get_schema(&hrana, token, table)?;

        // local にテーブルを作成（なければ）
        local
            .execute_batch(&schema)
            .map_err(|e| mk_err(format!("Failed to create table {}: {}", table, e)))?;

        if !offline {
            // pull: remote → local
            let rows = remote_select_all(&hrana, token, table)?;
            for row_sql in rows {
                local
                    .execute_batch(&row_sql)
                    .map_err(|e| mk_err(format!("Failed to insert row: {}", e)))?;
            }
        } else {
            // push: local → remote
            // local の全行を remote に INSERT OR REPLACE
            let local_rows = local_select_all(&local, table)?;
            for row_sql in local_rows {
                let requests = vec![
                    json!({ "type": "execute", "stmt": { "sql": row_sql } }),
                    json!({ "type": "close" }),
                ];
                hrana_pipeline(&hrana, token, None, &requests)?;
            }
        }
    }

    Ok(())
}

fn remote_get_tables(hrana_url: &str, token: &str) -> Result<Vec<String>, Error> {
    let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
    let requests = vec![
        json!({ "type": "execute", "stmt": { "sql": sql } }),
        json!({ "type": "close" }),
    ];
    let resp = hrana_pipeline(hrana_url, token, None, &requests)?;

    let mut tables = Vec::new();
    if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
        if let Some(first) = results.first() {
            let rows = extract_rows(first)?;
            for row in rows {
                if let Some((_, val)) = row.first() {
                    if let Some(name) = val.get("value").and_then(|v| v.as_str()) {
                        tables.push(name.to_string());
                    }
                }
            }
        }
    }
    Ok(tables)
}

fn remote_get_schema(hrana_url: &str, token: &str, table: &str) -> Result<String, Error> {
    let sql = format!(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='{}'",
        table.replace('\'', "''")
    );
    let requests = vec![
        json!({ "type": "execute", "stmt": { "sql": sql } }),
        json!({ "type": "close" }),
    ];
    let resp = hrana_pipeline(hrana_url, token, None, &requests)?;

    if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
        if let Some(first) = results.first() {
            let rows = extract_rows(first)?;
            if let Some(row) = rows.first() {
                if let Some((_, val)) = row.first() {
                    if let Some(schema) = val.get("value").and_then(|v| v.as_str()) {
                        return Ok(format!("CREATE TABLE IF NOT EXISTS {};", schema.trim_end_matches(';')));
                    }
                }
            }
        }
    }
    Err(mk_err(format!("Could not get schema for table: {}", table)))
}

fn remote_select_all(hrana_url: &str, token: &str, table: &str) -> Result<Vec<String>, Error> {
    let sql = format!("SELECT * FROM \"{}\"", table.replace('"', "\"\""));
    let requests = vec![
        json!({ "type": "execute", "stmt": { "sql": sql } }),
        json!({ "type": "close" }),
    ];
    let resp = hrana_pipeline(hrana_url, token, None, &requests)?;

    let mut sqls = Vec::new();
    if let Some(results) = resp.get("results").and_then(|r| r.as_array()) {
        if let Some(first) = results.first() {
            // カラム名を取得
            let response = first
                .get("response")
                .and_then(|r| r.get("result"))
                .ok_or_else(|| mk_err("Invalid response"))?;
            let cols = response
                .get("cols")
                .and_then(|c| c.as_array())
                .ok_or_else(|| mk_err("Missing cols"))?;
            let col_names: Vec<String> = cols
                .iter()
                .map(|c| {
                    c.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("?")
                        .to_string()
                })
                .collect();

            let rows = response
                .get("rows")
                .and_then(|r| r.as_array())
                .ok_or_else(|| mk_err("Missing rows"))?;

            for row in rows {
                let cells = row.as_array().ok_or_else(|| mk_err("Row is not array"))?;
                let col_list = col_names
                    .iter()
                    .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(", ");
                let val_list = cells
                    .iter()
                    .map(|cell| hrana_cell_to_sql_literal(cell))
                    .collect::<Vec<_>>()
                    .join(", ");
                sqls.push(format!(
                    "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({});",
                    table.replace('"', "\"\""),
                    col_list,
                    val_list
                ));
            }
        }
    }
    Ok(sqls)
}

fn local_select_all(
    conn: &rusqlite::Connection,
    table: &str,
) -> Result<Vec<String>, Error> {
    let sql = format!("SELECT * FROM \"{}\"", table.replace('"', "\"\""));
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| mk_err(format!("prepare error: {}", e)))?;

    let col_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let col_list = col_names
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let rows = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..col_names.len() {
                let val: rusqlite::types::Value = row.get(i)?;
                vals.push(val);
            }
            Ok(vals)
        })
        .map_err(|e| mk_err(format!("query error: {}", e)))?;

    let mut sqls = Vec::new();
    for row in rows {
        let vals = row.map_err(|e| mk_err(format!("row error: {}", e)))?;
        let val_list = vals
            .iter()
            .map(|v| rusqlite_value_to_sql_literal(v))
            .collect::<Vec<_>>()
            .join(", ");
        sqls.push(format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({});",
            table.replace('"', "\"\""),
            col_list,
            val_list
        ));
    }
    Ok(sqls)
}

fn hrana_cell_to_sql_literal(cell: &JsonValue) -> String {
    if cell.is_null() {
        return "NULL".to_string();
    }
    let typ = cell.get("type").and_then(|t| t.as_str()).unwrap_or("null");
    let val = cell.get("value");
    match typ {
        "null" => "NULL".to_string(),
        "integer" | "float" => val
            .and_then(|v| v.as_str())
            .unwrap_or("NULL")
            .to_string(),
        "text" => {
            let s = val.and_then(|v| v.as_str()).unwrap_or("");
            format!("'{}'", s.replace('\'', "''"))
        }
        "blob" => {
            let s = val.and_then(|v| v.as_str()).unwrap_or("");
            format!("X'{}'", s)
        }
        _ => "NULL".to_string(),
    }
}

fn rusqlite_value_to_sql_literal(val: &rusqlite::types::Value) -> String {
    match val {
        rusqlite::types::Value::Null => "NULL".to_string(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => f.to_string(),
        rusqlite::types::Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        rusqlite::types::Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("X'{}'", hex)
        }
    }
}

// -----------------------------------------------------------------------
// fork 後の子プロセスで呼ぶ（現実装では no-op）
// -----------------------------------------------------------------------

fn reinitialize_runtime(_ruby: &Ruby) -> Result<(), Error> {
    // tokio を使わないので何もしなくてよい
    Ok(())
}

// -----------------------------------------------------------------------
// Magnus init — Ruby 拡張のエントリポイント
// -----------------------------------------------------------------------

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("TursoLibsql")?;

    // TursoLibsql::Database
    let db_class = module.define_class("Database", ruby.class_object())?;
    db_class.define_singleton_method("new_remote", function!(TursoDatabase::new_remote, 2))?;
    db_class.define_singleton_method(
        "new_remote_replica",
        function!(TursoDatabase::new_remote_replica, 4),
    )?;
    db_class.define_singleton_method("new_synced", function!(TursoDatabase::new_synced, 4))?;
    db_class.define_method("sync", method!(TursoDatabase::sync, 0))?;
    db_class.define_method("connect", method!(TursoDatabase::connect, 0))?;

    // TursoLibsql::Connection
    let conn_class = module.define_class("Connection", ruby.class_object())?;
    conn_class.define_singleton_method("new", function!(TursoConnection::new, 2))?;
    conn_class.define_method("execute", method!(TursoConnection::execute, 1))?;
    conn_class.define_method("query", method!(TursoConnection::query, 1))?;
    conn_class.define_method(
        "execute_with_params",
        method!(TursoConnection::execute_with_params, 2),
    )?;
    conn_class.define_method(
        "begin_transaction",
        method!(TursoConnection::begin_transaction, 0),
    )?;
    conn_class.define_method(
        "commit_transaction",
        method!(TursoConnection::commit_transaction, 0),
    )?;
    conn_class.define_method(
        "rollback_transaction",
        method!(TursoConnection::rollback_transaction, 0),
    )?;
    conn_class.define_method(
        "last_insert_rowid",
        method!(TursoConnection::last_insert_rowid, 0),
    )?;

    // TursoLibsql.reinitialize_runtime!
    module.define_singleton_method(
        "reinitialize_runtime!",
        function!(reinitialize_runtime, 0),
    )?;

    Ok(())
}
