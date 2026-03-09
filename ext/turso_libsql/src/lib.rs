use magnus::{function, method, prelude::*, Error, Ruby};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::Duration;
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
fn mk_err(msg: impl std::fmt::Display) -> Error {
    let ruby = Ruby::get().expect("called outside Ruby thread");
    Error::new(ruby.exception_runtime_error(), msg.to_string())
}

// -----------------------------------------------------------------------
// TursoDatabase — Database を保持するラッパー（sync のために必要）
// -----------------------------------------------------------------------

#[magnus::wrap(class = "TursoLibsql::Database", free_immediately, size)]
struct TursoDatabase {
    inner: Arc<libsql::Database>,
}

impl TursoDatabase {
    /// リモート接続用 Database を作成（既存の remote モード）
    fn new_remote(url: String, token: String) -> Result<Self, Error> {
        let db = runtime().block_on(async {
            libsql::Builder::new_remote(url, token)
                .build()
                .await
                .map_err(mk_err)
        })?;
        Ok(Self { inner: Arc::new(db) })
    }

    /// Embedded Replica 用 Database を作成
    /// path: ローカル DB ファイルパス
    /// url: Turso リモート URL (libsql://...)
    /// token: 認証トークン
    /// sync_interval_secs: バックグラウンド自動同期間隔（秒）。0 なら手動のみ
    fn new_remote_replica(
        path: String,
        url: String,
        token: String,
        sync_interval_secs: u64,
    ) -> Result<Self, Error> {
        let db = runtime().block_on(async {
            let mut builder = libsql::Builder::new_remote_replica(path, url, token);
            if sync_interval_secs > 0 {
                builder = builder.sync_interval(Duration::from_secs(sync_interval_secs));
            }
            builder.build().await.map_err(mk_err)
        })?;
        Ok(Self { inner: Arc::new(db) })
    }

    /// Offline write 用 Database を作成
    /// write はローカルに書いてすぐ返す。sync() でまとめてリモートへ反映する。
    /// path: ローカル DB ファイルパス
    /// url: Turso リモート URL (libsql://...)
    /// token: 認証トークン
    /// sync_interval_secs: バックグラウンド自動同期間隔（秒）。0 なら手動のみ
    fn new_synced(
        path: String,
        url: String,
        token: String,
        sync_interval_secs: u64,
    ) -> Result<Self, Error> {
        let db = runtime().block_on(async {
            let mut builder = libsql::Builder::new_synced_database(path, url, token);
            if sync_interval_secs > 0 {
                builder = builder.sync_interval(Duration::from_secs(sync_interval_secs));
            }
            builder.build().await.map_err(mk_err)
        })?;
        Ok(Self { inner: Arc::new(db) })
    }

    /// リモートから最新フレームを手動で同期する（pull）
    /// offline モードでは write もまとめてリモートへ push される
    fn sync(&self) -> Result<(), Error> {
        let db = Arc::clone(&self.inner);
        runtime().block_on(async move {
            db.sync().await.map_err(mk_err)
        })?;
        Ok(())
    }

    /// この Database から Connection を取得して TursoConnection を返す
    fn connect(&self) -> Result<TursoConnection, Error> {
        let conn = self.inner.connect().map_err(mk_err)?;
        Ok(TursoConnection {
            inner: Arc::new(conn),
        })
    }
}

// -----------------------------------------------------------------------
// TursoConnection — Ruby に公開する接続オブジェクト
// -----------------------------------------------------------------------

#[magnus::wrap(class = "TursoLibsql::Connection", free_immediately, size)]
struct TursoConnection {
    inner: Arc<libsql::Connection>,
}

impl TursoConnection {
    /// 新しいリモート接続を作成する（Ruby: TursoLibsql::Connection.new(url, token)）
    /// 後方互換のために残す。内部では TursoDatabase を経由する
    fn new(url: String, token: String) -> Result<Self, Error> {
        let db = runtime().block_on(async {
            libsql::Builder::new_remote(url, token)
                .build()
                .await
                .map_err(mk_err)
        })?;
        let conn = db.connect().map_err(mk_err)?;
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
    fn query(&self, sql: String) -> Result<magnus::RArray, Error> {
        let conn = Arc::clone(&self.inner);

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

    // TursoLibsql::Database
    let db_class = module.define_class("Database", ruby.class_object())?;
    db_class.define_singleton_method("new_remote", function!(TursoDatabase::new_remote, 2))?;
    db_class.define_singleton_method(
        "new_remote_replica",
        function!(TursoDatabase::new_remote_replica, 4),
    )?;
    db_class.define_singleton_method(
        "new_synced",
        function!(TursoDatabase::new_synced, 4),
    )?;
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

    Ok(())
}
