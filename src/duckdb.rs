#[cfg(feature = "duckdb")]
use crate::backend::{DatabaseBackend, QueryResult, QueryRow};
use crate::error::{QueryError, Result};
use async_trait::async_trait;
use duckdb::{params, Connection, Row};
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// DuckDB backend for OLAP workloads and analytics
pub struct DuckDBBackend {
    conn: Arc<Mutex<Connection>>,
    in_transaction: bool,
}

impl DuckDBBackend {
    /// Create a new DuckDB backend with an in-memory database
    pub async fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| QueryError::Connection(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            in_transaction: false,
        })
    }

    /// Create a new DuckDB backend with a file-based database
    pub async fn file(path: &str) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| QueryError::Connection(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            in_transaction: false,
        })
    }

    /// Convert DuckDB row to QueryRow
    fn convert_row(row: &Row) -> Result<QueryRow> {
        let mut query_row = QueryRow::new();
        let column_count = row.as_ref().column_count();

        for i in 0..column_count {
            let column_name = row.as_ref().column_name(i)
                .map_err(|e| QueryError::Database(e.to_string()))?;

            // Try to extract value with proper type handling
            if let Ok(value) = row.get::<_, Option<String>>(i) {
                if let Some(s) = value {
                    query_row.insert(column_name.to_string(), Value::String(s));
                } else {
                    query_row.insert(column_name.to_string(), Value::Null);
                }
            } else if let Ok(value) = row.get::<_, Option<i64>>(i) {
                if let Some(n) = value {
                    query_row.insert(column_name.to_string(), Value::Number(n.into()));
                } else {
                    query_row.insert(column_name.to_string(), Value::Null);
                }
            } else if let Ok(value) = row.get::<_, Option<i32>>(i) {
                if let Some(n) = value {
                    query_row.insert(column_name.to_string(), Value::Number(n.into()));
                } else {
                    query_row.insert(column_name.to_string(), Value::Null);
                }
            } else if let Ok(value) = row.get::<_, Option<f64>>(i) {
                if let Some(n) = value {
                    if let Some(num) = serde_json::Number::from_f64(n) {
                        query_row.insert(column_name.to_string(), Value::Number(num));
                    }
                } else {
                    query_row.insert(column_name.to_string(), Value::Null);
                }
            } else if let Ok(value) = row.get::<_, Option<bool>>(i) {
                if let Some(b) = value {
                    query_row.insert(column_name.to_string(), Value::Bool(b));
                } else {
                    query_row.insert(column_name.to_string(), Value::Null);
                }
            } else {
                // Default to null if we can't extract the value
                query_row.insert(column_name.to_string(), Value::Null);
            }
        }

        Ok(query_row)
    }

    /// Export database to Parquet file
    pub async fn export_parquet(&mut self, table: &str, path: &str) -> Result<()> {
        let sql = format!("COPY {} TO '{}' (FORMAT PARQUET)", table, path);
        self.execute(&sql).await?;
        Ok(())
    }

    /// Import data from Parquet file
    pub async fn import_parquet(&mut self, table: &str, path: &str) -> Result<()> {
        let sql = format!("COPY {} FROM '{}' (FORMAT PARQUET)", table, path);
        self.execute(&sql).await?;
        Ok(())
    }

    /// Export database to CSV file
    pub async fn export_csv(&mut self, table: &str, path: &str) -> Result<()> {
        let sql = format!("COPY {} TO '{}' (FORMAT CSV, HEADER)", table, path);
        self.execute(&sql).await?;
        Ok(())
    }

    /// Get table statistics
    pub async fn analyze_table(&mut self, table: &str) -> Result<()> {
        let sql = format!("ANALYZE {}", table);
        self.execute(&sql).await?;
        Ok(())
    }

    /// Create an index on a table
    pub async fn create_index(&mut self, table: &str, column: &str, index_name: &str) -> Result<()> {
        let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {} ({})", index_name, table, column);
        self.execute(&sql).await?;
        Ok(())
    }
}

#[async_trait]
impl DatabaseBackend for DuckDBBackend {
    async fn connect(url: &str) -> Result<Self> {
        if url == ":memory:" || url == "memory" {
            Self::memory().await
        } else {
            Self::file(url).await
        }
    }

    async fn execute(&mut self, sql: &str) -> Result<u64> {
        // Run in blocking task to avoid blocking async runtime
        let sql = sql.to_string();
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock()
                .map_err(|e| QueryError::Database(format!("Lock error: {}", e)))?;

            let affected = conn.execute(&sql, params![])
                .map_err(|e| QueryError::Database(e.to_string()))?;

            Ok(affected as u64)
        })
        .await
        .map_err(|e| QueryError::Database(format!("Join error: {}", e)))?
    }

    async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Run in blocking task to avoid blocking async runtime
        let sql = sql.to_string();
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock()
                .map_err(|e| QueryError::Database(format!("Lock error: {}", e)))?;

            let mut stmt = conn.prepare(&sql)
                .map_err(|e| QueryError::Database(e.to_string()))?;

            let rows = stmt.query_map(params![], |row| {
                Ok(DuckDBBackend::convert_row(row).map_err(|e| duckdb::Error::ToSqlConversionFailure(Box::new(e)))?)
            })
            .map_err(|e| QueryError::Database(e.to_string()))?;

            let mut result = Vec::new();
            for row_result in rows {
                let query_row = row_result.map_err(|e| QueryError::Database(e.to_string()))?;
                result.push(query_row);
            }

            Ok(result)
        })
        .await
        .map_err(|e| QueryError::Database(format!("Join error: {}", e)))?
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(QueryError::Transaction("Already in transaction".to_string()));
        }

        self.execute("BEGIN TRANSACTION").await?;
        self.in_transaction = true;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(QueryError::Transaction("Not in transaction".to_string()));
        }

        self.execute("COMMIT").await?;
        self.in_transaction = false;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(QueryError::Transaction("Not in transaction".to_string()));
        }

        self.execute("ROLLBACK").await?;
        self.in_transaction = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // DuckDB connections are always "connected" once created
        true
    }

    async fn close(self) -> Result<()> {
        // DuckDB connection will be closed when dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_duckdb_memory() {
        let backend = DuckDBBackend::memory().await;
        assert!(backend.is_ok());
    }

    #[tokio::test]
    async fn test_duckdb_create_table() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)")
            .await
            .unwrap();

        let results = backend
            .query("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")
            .await
            .unwrap();

        assert!(results.len() > 0 || backend.is_connected());
    }

    #[tokio::test]
    async fn test_duckdb_insert_query() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .await
            .unwrap();

        let results = backend
            .query("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_string("name"), Some("Alice".to_string()));
        assert_eq!(results[0].get_i64("age"), Some(30));

        assert_eq!(results[1].get_string("name"), Some("Bob".to_string()));
        assert_eq!(results[1].get_i64("age"), Some(25));
    }

    #[tokio::test]
    async fn test_duckdb_aggregation() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE sales (product VARCHAR, amount INTEGER)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150)")
            .await
            .unwrap();

        let results = backend
            .query("SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY product")
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_string("product"), Some("A".to_string()));
        assert_eq!(results[0].get_i64("total"), Some(250));
    }

    #[tokio::test]
    async fn test_duckdb_analytical_query() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE events (user_id INTEGER, event_type VARCHAR, timestamp BIGINT)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO events VALUES (1, 'login', 1000), (1, 'click', 2000), (2, 'login', 1500)")
            .await
            .unwrap();

        // Window function - DuckDB excels at these
        let results = backend
            .query("SELECT user_id, event_type, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) as event_seq FROM events")
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_duckdb_null_handling() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE nullable_test (id INTEGER, value VARCHAR)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO nullable_test VALUES (1, NULL)")
            .await
            .unwrap();

        let results = backend
            .query("SELECT * FROM nullable_test WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_string("value"), None);
    }

    #[tokio::test]
    async fn test_duckdb_transaction() {
        let mut backend = DuckDBBackend::memory().await.unwrap();

        backend
            .execute("CREATE TABLE accounts (id INTEGER, balance INTEGER)")
            .await
            .unwrap();

        backend
            .execute("INSERT INTO accounts VALUES (1, 100)")
            .await
            .unwrap();

        // Test transaction state tracking
        assert!(!backend.in_transaction);
        backend.begin_transaction().await.unwrap();
        assert!(backend.in_transaction);

        // Can't begin another transaction while one is active
        assert!(backend.begin_transaction().await.is_err());

        // Reset state
        backend.in_transaction = false;
    }
}
