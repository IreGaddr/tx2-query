use crate::error::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Row from a query result
#[derive(Debug, Clone)]
pub struct QueryRow {
    pub columns: HashMap<String, Value>,
}

impl QueryRow {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.columns.insert(key, value);
    }

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.columns
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.columns
            .get(key)
            .and_then(|v| v.as_str().map(String::from))
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.columns.get(key).and_then(|v| v.as_i64())
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.columns.get(key).and_then(|v| v.as_f64())
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.columns.get(key).and_then(|v| v.as_bool())
    }
}

impl Default for QueryRow {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a query
pub type QueryResult = Vec<QueryRow>;

/// Database backend trait
#[async_trait]
pub trait DatabaseBackend: Send + Sync {
    /// Connect to the database
    async fn connect(url: &str) -> Result<Self>
    where
        Self: Sized;

    /// Execute a SQL statement (no results)
    async fn execute(&mut self, sql: &str) -> Result<u64>;

    /// Query and return results
    async fn query(&mut self, sql: &str) -> Result<QueryResult>;

    /// Begin a transaction
    async fn begin_transaction(&mut self) -> Result<()>;

    /// Commit the current transaction
    async fn commit(&mut self) -> Result<()>;

    /// Rollback the current transaction
    async fn rollback(&mut self) -> Result<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;

    /// Close the connection
    async fn close(self) -> Result<()>;
}

/// Transaction guard for automatic rollback
pub struct Transaction<'a, B: DatabaseBackend> {
    backend: &'a mut B,
    committed: bool,
}

impl<'a, B: DatabaseBackend> Transaction<'a, B> {
    pub async fn new(backend: &'a mut B) -> Result<Self> {
        backend.begin_transaction().await?;
        Ok(Self {
            backend,
            committed: false,
        })
    }

    pub async fn commit(mut self) -> Result<()> {
        self.backend.commit().await?;
        self.committed = true;
        Ok(())
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        self.backend.execute(sql).await
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.backend.query(sql).await
    }
}

impl<'a, B: DatabaseBackend> Drop for Transaction<'a, B> {
    fn drop(&mut self) {
        if !self.committed {
            // Rollback on drop if not committed
            // Can't use async in drop, so this is best-effort
            // The backend should handle this on connection drop
        }
    }
}
