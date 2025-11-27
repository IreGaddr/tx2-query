use thiserror::Error;

pub type Result<T> = std::result::Result<T, QueryError>;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Type mapping error: {0}")]
    TypeMapping(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Component not registered: {0}")]
    ComponentNotRegistered(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "postgres")]
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

impl From<tx2_link::LinkError> for QueryError {
    fn from(err: tx2_link::LinkError) -> Self {
        QueryError::Serialization(err.to_string())
    }
}
