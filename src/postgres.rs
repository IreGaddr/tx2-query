#[cfg(feature = "postgres")]
use crate::backend::{DatabaseBackend, QueryResult, QueryRow};
use crate::error::{QueryError, Result};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Row, Column};

pub struct PostgresBackend {
    pool: PgPool,
    in_transaction: bool,
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend with connection pool
    pub async fn new(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;

        Ok(Self {
            pool,
            in_transaction: false,
        })
    }

    /// Convert PostgreSQL row to QueryRow
    fn convert_row(row: &PgRow) -> QueryRow {
        let mut query_row = QueryRow::new();

        for column in row.columns() {
            let column_name = column.name();

            // Try to extract value as JSON
            if let Ok(value) = row.try_get::<Value, _>(column_name) {
                query_row.insert(column_name.to_string(), value);
            } else if let Ok(value) = row.try_get::<String, _>(column_name) {
                query_row.insert(column_name.to_string(), Value::String(value));
            } else if let Ok(value) = row.try_get::<i64, _>(column_name) {
                query_row.insert(column_name.to_string(), Value::Number(value.into()));
            } else if let Ok(value) = row.try_get::<i32, _>(column_name) {
                query_row.insert(column_name.to_string(), Value::Number(value.into()));
            } else if let Ok(value) = row.try_get::<f64, _>(column_name) {
                if let Some(num) = serde_json::Number::from_f64(value) {
                    query_row.insert(column_name.to_string(), Value::Number(num));
                }
            } else if let Ok(value) = row.try_get::<bool, _>(column_name) {
                query_row.insert(column_name.to_string(), Value::Bool(value));
            } else {
                // Default to null if we can't extract the value
                query_row.insert(column_name.to_string(), Value::Null);
            }
        }

        query_row
    }
}

#[async_trait]
impl DatabaseBackend for PostgresBackend {
    async fn connect(url: &str) -> Result<Self> {
        Self::new(url).await
    }

    async fn execute(&mut self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;

        let result = rows.iter().map(Self::convert_row).collect();

        Ok(result)
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(QueryError::Transaction(
                "Already in transaction".to_string(),
            ));
        }

        // Start transaction using SAVEPOINT approach
        self.execute("BEGIN").await?;
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
        !self.pool.is_closed()
    }

    async fn close(self) -> Result<()> {
        self.pool.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_connection() {
        let backend = PostgresBackend::connect("postgresql://localhost/test").await;
        assert!(backend.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_query() {
        let mut backend = PostgresBackend::connect("postgresql://localhost/test")
            .await
            .unwrap();

        // Create test table
        backend
            .execute("CREATE TEMPORARY TABLE test_table (id BIGINT PRIMARY KEY, name TEXT)")
            .await
            .unwrap();

        // Insert data
        backend
            .execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice')")
            .await
            .unwrap();

        // Query data
        let results = backend
            .query("SELECT * FROM test_table WHERE id = 1")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_i64("id"), Some(1));
        assert_eq!(results[0].get_string("name"), Some("Alice".to_string()));
    }
}
