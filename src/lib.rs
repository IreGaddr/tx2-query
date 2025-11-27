//! tx2-query - SQL Analytics Layer for TX-2 ECS
//!
//! tx2-query provides a SQL side-car database for TX-2 applications, enabling
//! advanced analytics, complex queries, and external tool integration while keeping
//! the ECS world as the source of truth.
//!
//! # Architecture
//!
//! - **ECS World → SQL Database**: One-way synchronization from ECS to SQL
//! - **Component → Table Mapping**: Automatic schema generation from component types
//! - **Incremental Sync**: Track and sync only changed entities/components
//! - **Multiple Backends**: PostgreSQL, SQLite, DuckDB, and more
//!
//! # Features
//!
//! - **Schema Generation**: Automatic DDL creation from component definitions
//! - **Query Builder**: Ergonomic SQL query construction
//! - **Sync API**: Incremental synchronization with batching and transactions
//! - **Read-Only Projection**: Analytics layer, not an ORM
//! - **Backend Abstraction**: Support for multiple SQL databases
//!
//! # Example
//!
//! ```rust,no_run
//! use tx2_query::*;
//! use tx2_query::schema::{SchemaGenerator, SqlType};
//! use tx2_query::builder::SelectBuilder;
//!
//! #[derive(Debug)]
//! struct Player;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create schema generator
//!     let mut schema_gen = SchemaGenerator::new();
//!     schema_gen.register::<Player>(
//!         "Player",
//!         vec![
//!             ("name", SqlType::Text, false),
//!             ("email", SqlType::Text, false),
//!             ("score", SqlType::Integer, false),
//!         ],
//!     )?;
//!
//!     // Connect to database (requires postgres feature)
//!     #[cfg(feature = "postgres")]
//!     {
//!         use tx2_query::postgres::PostgresBackend;
//!         let backend = PostgresBackend::connect("postgresql://localhost/mydb").await?;
//!         let sync = tx2_query::sync::QuerySync::new(backend, schema_gen);
//!
//!         // Initialize schema
//!         sync.initialize_schema().await?;
//!
//!         // Build and execute queries
//!         let query = SelectBuilder::new("Player")
//!             .select(vec!["name", "score"])
//!             .where_gt("score", serde_json::json!(1000))
//!             .order_desc("score")
//!             .limit(10)
//!             .build()?;
//!
//!         let results = sync.query(&query).await?;
//!         println!("Top players: {:?}", results);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Backend Support
//!
//! Enable backends via Cargo features:
//!
//! ```toml
//! [dependencies]
//! tx2-query = { version = "0.1", features = ["postgres", "sqlite", "duckdb"] }
//! ```
//!
//! # Philosophy
//!
//! tx2-query is designed as an analytics layer, NOT an ORM:
//!
//! - ECS world is always the source of truth
//! - SQL database is a read-only projection for analytics
//! - No bi-directional sync - changes flow ECS → SQL only
//! - Optimized for analytical queries, not transactional workloads

pub mod backend;
pub mod builder;
pub mod error;
pub mod schema;
pub mod sync;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "duckdb")]
pub mod duckdb;

pub use backend::{DatabaseBackend, QueryResult, QueryRow, Transaction};
pub use builder::{
    AggregateFunc, ComparisonOp, Condition, DeleteBuilder, JoinType, LogicalOp, SelectBuilder,
    SortDirection, UpdateBuilder,
};
pub use error::{QueryError, Result};
pub use schema::{ColumnDef, IndexDef, SchemaGenerator, SqlType, TableSchema};
pub use sync::{ComponentChange, EntityChange, QuerySync, SyncBatch, SyncConfig};

#[cfg(feature = "postgres")]
pub use postgres::PostgresBackend;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteBackend;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDBBackend;

/// Prelude for common imports
pub mod prelude {
    pub use crate::backend::{DatabaseBackend, QueryResult, QueryRow};
    pub use crate::builder::{
        ComparisonOp, Condition, SelectBuilder, SortDirection, UpdateBuilder,
    };
    pub use crate::error::{QueryError, Result};
    pub use crate::schema::{SchemaGenerator, SqlType};
    pub use crate::sync::{QuerySync, SyncConfig};

    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresBackend;

    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqliteBackend;

    #[cfg(feature = "duckdb")]
    pub use crate::duckdb::DuckDBBackend;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_exports() {
        // Ensure all main types are exported
        let _schema = SchemaGenerator::new();
        let _sql_type = SqlType::Text;
        let _comp_op = ComparisonOp::Eq;
        let _log_op = LogicalOp::And;
    }
}
