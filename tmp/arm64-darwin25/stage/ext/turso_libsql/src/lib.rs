use magnus::{function, method, prelude::*, Error, Ruby};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::runtime::Runtime;

// グローバル Tokio ランタイム（libsql は async API のため必要）
static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

/// async ブロック内から Ruby の RuntimeError を生成するヘルパー
/// （async ブロックは &Ruby を借用できないため Ruby::get() を使う）
fn mk_err(msg: impl std::fmt::Display) -> Error {
    let ruby = Ruby::get().expect("called outside Ruby thread");
    Error::new(ruby.exception_runtime_error(), msg.to_string())
}

// -----------------------------------------------------------------------
// TursoConnection — Ruby に公開する接続オブジェクト
// -----------------------------------------------------------------------

#[magnus::wrap(class = "TursoLibsql::Connection", free_immediately, size)]
struct TursoConnection {
    inner: Arc<libsql::Connection>,
}

impl TursoConnection {
    /// 新しい接続を作成する（Ruby: TursoLibsql::Connection.new(url, token)）
    fn new(url: String, token: String) -> Result<Self, Error> {
        let conn = runtime().block_on(async {
            let db = libsql::Builder::new_remote(url, token)
                .build()
                .await
                .map_err(mk_err)?;

            db.connect().map_err(mk_err)
        })?;

        Ok(Self {
            inner: Arc::new(conn),
        })
    }

    /// SQL を実行し、影響を受けた行数を返す（INSERT/UPDATE/DELETE 用）
    fn execute(&self, sql: String) -> Result<u64, Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            conn.execute(&sql, ()).await.map_err(mk_err)
        })
    }

    /// SQL を実行し、結果を Array of Hash で返す（SELECT 用）
    ///
    /// 返り値: `[{ "col" => value, ... }, ...]`
    fn query(&self, sql: String) -> Result<magnus::RArray, Error> {
        let conn = Arc::clone(&self.inner);

        // async ブロック内でデータを Rust 型として収集する
        let rows_data: Vec<Vec<(String, libsql::Value)>> =
            runtime().block_on(async move {
                let mut rows = conn.query(&sql, ()).await.map_err(mk_err)?;
                let mut result: Vec<Vec<(String, libsql::Value)>> = Vec::new();

                while let Some(row) = rows.next().await.map_err(mk_err)? {
                    let col_count = rows.column_count();
                    let mut record: Vec<(String, libsql::Value)> =
                        Vec::with_capacity(col_count as usize);

                    for i in 0..col_count {
                        let name = rows.column_name(i).unwrap_or("?").to_string();
                        let val = row.get_value(i).map_err(mk_err)?;
                        record.push((name, val));
                    }
                    result.push(record);
                }
                Ok::<_, Error>(result)
            })?;

        // 同期部分で Ruby オブジェクトに変換する
        let ruby = Ruby::get().expect("called outside Ruby thread");
        let outer = ruby.ary_new_capa(rows_data.len());
        for record in rows_data {
            let hash = ruby.hash_new();
            for (col, val) in record {
                let ruby_key = ruby.str_new(&col);
                let ruby_val = libsql_value_to_ruby(&ruby, val)?;
                hash.aset(ruby_key, ruby_val)?;
            }
            outer.push(hash)?;
        }
        Ok(outer)
    }

    /// プリペアドステートメントで SQL を実行（パラメータ付き）
    fn execute_with_params(&self, sql: String, params: Vec<String>) -> Result<u64, Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            let params: Vec<libsql::Value> =
                params.into_iter().map(libsql::Value::Text).collect();
            conn.execute(&sql, libsql::params_from_iter(params))
                .await
                .map_err(mk_err)
        })
    }

    /// トランザクションを開始する
    fn begin_transaction(&self) -> Result<(), Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            conn.execute("BEGIN", ()).await.map_err(mk_err)
        })?;
        Ok(())
    }

    /// トランザクションをコミットする
    fn commit_transaction(&self) -> Result<(), Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            conn.execute("COMMIT", ()).await.map_err(mk_err)
        })?;
        Ok(())
    }

    /// トランザクションをロールバックする
    fn rollback_transaction(&self) -> Result<(), Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            conn.execute("ROLLBACK", ()).await.map_err(mk_err)
        })?;
        Ok(())
    }

    /// 最後に挿入した行の rowid を返す
    fn last_insert_rowid(&self) -> Result<i64, Error> {
        let conn = Arc::clone(&self.inner);
        runtime().block_on(async move {
            let mut rows = conn
                .query("SELECT last_insert_rowid()", ())
                .await
                .map_err(mk_err)?;

            if let Some(row) = rows.next().await.map_err(mk_err)? {
                row.get::<i64>(0).map_err(mk_err)
            } else {
                Ok(0)
            }
        })
    }
}

// -----------------------------------------------------------------------
// libsql::Value → Ruby Value 変換
// -----------------------------------------------------------------------

fn libsql_value_to_ruby(ruby: &Ruby, val: libsql::Value) -> Result<magnus::Value, Error> {
    match val {
        libsql::Value::Null => Ok(ruby.qnil().as_value()),
        libsql::Value::Integer(i) => Ok(ruby.integer_from_i64(i).as_value()),
        libsql::Value::Real(f) => Ok(ruby.float_from_f64(f).as_value()),
        libsql::Value::Text(s) => Ok(ruby.str_new(&s).as_value()),
        libsql::Value::Blob(b) => Ok(ruby.str_from_slice(&b).as_value()),
    }
}

// -----------------------------------------------------------------------
// Magnus init — Ruby 拡張のエントリポイント
// -----------------------------------------------------------------------

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("TursoLibsql")?;

    let conn_class = module.define_class("Connection", ruby.class_object())?;

    // function! / method! の第2引数は Ruby 側から渡す引数の数
    // （&Ruby は magnus が自動注入するのでカウントしない）
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

    Ok(())
}
