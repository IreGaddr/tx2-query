use crate::backend::{DatabaseBackend, QueryResult};
use crate::error::{QueryError, Result};
use crate::schema::SchemaGenerator;
use serde_json::Value;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Entity change tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityChange {
    /// Entity was created
    Created(u64),
    /// Entity was modified
    Modified(u64),
    /// Entity was deleted
    Deleted(u64),
}

/// Component change tracking
#[derive(Debug, Clone)]
pub struct ComponentChange {
    pub entity_id: u64,
    pub component_type: TypeId,
    pub component_name: String,
    pub data: Option<HashMap<String, Value>>,
}

/// Batch of changes to sync
#[derive(Debug, Clone)]
pub struct SyncBatch {
    pub created: Vec<u64>,
    pub modified: HashMap<TypeId, Vec<ComponentChange>>,
    pub deleted: Vec<u64>,
}

impl SyncBatch {
    pub fn new() -> Self {
        Self {
            created: Vec::new(),
            modified: HashMap::new(),
            deleted: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.created.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
    }

    pub fn clear(&mut self) {
        self.created.clear();
        self.modified.clear();
        self.deleted.clear();
    }
}

impl Default for SyncBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for query sync
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Whether to sync in background
    pub background_sync: bool,
    /// Whether to use transactions for batches
    pub use_transactions: bool,
    /// Maximum number of retries on failure
    pub max_retries: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            background_sync: true,
            use_transactions: false,  // Disabled by default due to connection pooling limitations
            max_retries: 3,
        }
    }
}

/// QuerySync manages synchronization between ECS world and SQL database
pub struct QuerySync<B: DatabaseBackend> {
    backend: Arc<RwLock<B>>,
    schema_generator: Arc<RwLock<SchemaGenerator>>,
    config: SyncConfig,
    pending_batch: Arc<RwLock<SyncBatch>>,
    synced_entities: Arc<RwLock<HashSet<u64>>>,
}

impl<B: DatabaseBackend> QuerySync<B> {
    /// Create a new QuerySync instance
    pub fn new(backend: B, schema_generator: SchemaGenerator) -> Self {
        Self {
            backend: Arc::new(RwLock::new(backend)),
            schema_generator: Arc::new(RwLock::new(schema_generator)),
            config: SyncConfig::default(),
            pending_batch: Arc::new(RwLock::new(SyncBatch::new())),
            synced_entities: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Create a new QuerySync instance with custom config
    pub fn with_config(backend: B, schema_generator: SchemaGenerator, config: SyncConfig) -> Self {
        Self {
            backend: Arc::new(RwLock::new(backend)),
            schema_generator: Arc::new(RwLock::new(schema_generator)),
            config,
            pending_batch: Arc::new(RwLock::new(SyncBatch::new())),
            synced_entities: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Initialize database schema
    pub async fn initialize_schema(&self) -> Result<()> {
        let schema_gen = self.schema_generator.read().await;
        let ddl = schema_gen.generate_ddl();

        let mut backend = self.backend.write().await;

        // Execute each DDL statement
        for statement in ddl.split(";") {
            let statement = statement.trim();
            if !statement.is_empty() {
                backend.execute(statement).await?;
            }
        }

        Ok(())
    }

    /// Track entity creation
    pub async fn track_entity_created(&self, entity_id: u64) -> Result<()> {
        let mut batch = self.pending_batch.write().await;
        batch.created.push(entity_id);

        if batch.created.len() >= self.config.batch_size {
            drop(batch);
            self.flush().await?;
        }

        Ok(())
    }

    /// Track component change
    pub async fn track_component_change(
        &self,
        entity_id: u64,
        component_type: TypeId,
        component_name: String,
        data: HashMap<String, Value>,
    ) -> Result<()> {
        let mut batch = self.pending_batch.write().await;

        let changes = batch.modified.entry(component_type).or_insert_with(Vec::new);
        changes.push(ComponentChange {
            entity_id,
            component_type,
            component_name,
            data: Some(data),
        });

        let total_changes: usize = batch.modified.values().map(|v| v.len()).sum();
        if total_changes >= self.config.batch_size {
            drop(batch);
            self.flush().await?;
        }

        Ok(())
    }

    /// Track entity deletion
    pub async fn track_entity_deleted(&self, entity_id: u64) -> Result<()> {
        let mut batch = self.pending_batch.write().await;
        batch.deleted.push(entity_id);

        if batch.deleted.len() >= self.config.batch_size {
            drop(batch);
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush pending changes to database
    pub async fn flush(&self) -> Result<()> {
        // Clone the batch and clear it immediately to avoid holding the lock
        let batch_to_apply = {
            let mut batch = self.pending_batch.write().await;

            if batch.is_empty() {
                return Ok(());
            }

            let batch_clone = batch.clone();
            batch.clear();
            batch_clone
        };

        let mut backend = self.backend.write().await;

        if self.config.use_transactions {
            backend.begin_transaction().await?;
        }

        let result = self.apply_batch(&mut backend, &batch_to_apply).await;

        match result {
            Ok(_) => {
                if self.config.use_transactions {
                    backend.commit().await?;
                }
                Ok(())
            }
            Err(e) => {
                if self.config.use_transactions {
                    backend.rollback().await?;
                }
                Err(e)
            }
        }
    }

    /// Apply a batch of changes to the database
    async fn apply_batch(&self, backend: &mut B, batch: &SyncBatch) -> Result<()> {
        let schema_gen = self.schema_generator.read().await;

        // Handle deletions first
        for entity_id in &batch.deleted {
            for component_name in schema_gen.list_components() {
                let delete_sql = format!("DELETE FROM {} WHERE entity_id = {}", component_name, entity_id);
                backend.execute(&delete_sql).await?;
            }

            let mut synced = self.synced_entities.write().await;
            synced.remove(entity_id);
        }

        // Handle component modifications (INSERT or UPDATE)
        for (component_type, changes) in &batch.modified {
            if changes.is_empty() {
                continue;
            }

            let _schema = schema_gen
                .get_schema(component_type)
                .ok_or_else(|| QueryError::ComponentNotRegistered(format!("{:?}", component_type)))?;

            for change in changes {
                let data = change
                    .data
                    .as_ref()
                    .ok_or_else(|| QueryError::Sync("Missing component data".to_string()))?;

                // Build column names and values
                let mut columns = vec!["entity_id".to_string()];
                let mut values = vec![change.entity_id.to_string()];

                for (key, value) in data {
                    columns.push(key.clone());
                    values.push(self.format_value(value));
                }

                // Check if entity already exists in this table
                let synced = self.synced_entities.read().await;
                let exists = synced.contains(&change.entity_id);
                drop(synced);

                if exists {
                    // UPDATE
                    let mut set_clauses = Vec::new();
                    for (key, value) in data {
                        set_clauses.push(format!("{} = {}", key, self.format_value(value)));
                    }

                    let update_sql = format!(
                        "UPDATE {} SET {} WHERE entity_id = {}",
                        change.component_name,
                        set_clauses.join(", "),
                        change.entity_id
                    );

                    backend.execute(&update_sql).await?;
                } else {
                    // INSERT
                    let insert_sql = format!(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (entity_id) DO UPDATE SET {}",
                        change.component_name,
                        columns.join(", "),
                        values.join(", "),
                        data.iter()
                            .map(|(k, v)| format!("{} = {}", k, self.format_value(v)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );

                    backend.execute(&insert_sql).await?;

                    let mut synced = self.synced_entities.write().await;
                    synced.insert(change.entity_id);
                }
            }
        }

        // Handle creations (mark entities as synced)
        for entity_id in &batch.created {
            let mut synced = self.synced_entities.write().await;
            synced.insert(*entity_id);
        }

        Ok(())
    }

    /// Format a JSON value for SQL insertion
    fn format_value(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string().to_uppercase(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Array(_) | Value::Object(_) => {
                format!("'{}'", serde_json::to_string(value).unwrap_or_default().replace('\'', "''"))
            }
        }
    }

    /// Query the database
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let mut backend = self.backend.write().await;
        backend.query(sql).await
    }

    /// Execute a SQL statement
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        let mut backend = self.backend.write().await;
        backend.execute(sql).await
    }

    /// Get the current sync configuration
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Check if backend is connected
    pub async fn is_connected(&self) -> bool {
        let backend = self.backend.read().await;
        backend.is_connected()
    }

    /// Get the number of synced entities
    pub async fn synced_entity_count(&self) -> usize {
        let synced = self.synced_entities.read().await;
        synced.len()
    }

    /// Get the number of pending changes
    pub async fn pending_change_count(&self) -> usize {
        let batch = self.pending_batch.read().await;
        batch.created.len()
            + batch.modified.values().map(|v| v.len()).sum::<usize>()
            + batch.deleted.len()
    }

    /// Clear all synced data from database
    pub async fn clear_all(&self) -> Result<()> {
        let schema_gen = self.schema_generator.read().await;
        let mut backend = self.backend.write().await;

        for component_name in schema_gen.list_components() {
            let truncate_sql = format!("DELETE FROM {}", component_name);
            backend.execute(&truncate_sql).await?;
        }

        let mut synced = self.synced_entities.write().await;
        synced.clear();

        Ok(())
    }

    /// Perform a full sync of all components for given entities
    pub async fn full_sync(
        &self,
        entities: Vec<(u64, TypeId, String, HashMap<String, Value>)>,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;

        if self.config.use_transactions {
            backend.begin_transaction().await?;
        }

        let result = async {
            for (entity_id, _component_type, component_name, data) in entities {
                let mut columns = vec!["entity_id".to_string()];
                let mut values = vec![entity_id.to_string()];

                for (key, value) in &data {
                    columns.push(key.clone());
                    values.push(self.format_value(value));
                }

                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (entity_id) DO UPDATE SET {}",
                    component_name,
                    columns.join(", "),
                    values.join(", "),
                    data.iter()
                        .map(|(k, v)| format!("{} = {}", k, self.format_value(v)))
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                backend.execute(&insert_sql).await?;

                let mut synced = self.synced_entities.write().await;
                synced.insert(entity_id);
            }

            Ok::<(), QueryError>(())
        }
        .await;

        match result {
            Ok(_) => {
                if self.config.use_transactions {
                    backend.commit().await?;
                }
                Ok(())
            }
            Err(e) => {
                if self.config.use_transactions {
                    backend.rollback().await?;
                }
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SqlType;
    use async_trait::async_trait;

    struct MockBackend {
        executed: Vec<String>,
        in_transaction: bool,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                executed: Vec::new(),
                in_transaction: false,
            }
        }
    }

    #[async_trait]
    impl DatabaseBackend for MockBackend {
        async fn connect(_url: &str) -> Result<Self> {
            Ok(Self::new())
        }

        async fn execute(&mut self, sql: &str) -> Result<u64> {
            self.executed.push(sql.to_string());
            Ok(1)
        }

        async fn query(&mut self, _sql: &str) -> Result<QueryResult> {
            Ok(vec![])
        }

        async fn begin_transaction(&mut self) -> Result<()> {
            self.in_transaction = true;
            Ok(())
        }

        async fn commit(&mut self) -> Result<()> {
            self.in_transaction = false;
            Ok(())
        }

        async fn rollback(&mut self) -> Result<()> {
            self.in_transaction = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TestComponent;

    #[tokio::test]
    async fn test_track_component_change() {
        let backend = MockBackend::new();
        let mut schema_gen = SchemaGenerator::new();
        schema_gen
            .register::<TestComponent>("Player", vec![("name", SqlType::Text, false)])
            .unwrap();

        let sync = QuerySync::with_config(
            backend,
            schema_gen,
            SyncConfig {
                batch_size: 10,
                use_transactions: false,
                ..Default::default()
            },
        );

        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Alice".to_string()));

        sync.track_component_change(1, TypeId::of::<TestComponent>(), "Player".to_string(), data)
            .await
            .unwrap();

        assert_eq!(sync.pending_change_count().await, 1);
    }

    #[tokio::test]
    async fn test_flush() {
        let backend = MockBackend::new();
        let mut schema_gen = SchemaGenerator::new();
        schema_gen
            .register::<TestComponent>("Player", vec![("name", SqlType::Text, false)])
            .unwrap();

        let sync = QuerySync::with_config(
            backend,
            schema_gen,
            SyncConfig {
                use_transactions: false,
                ..Default::default()
            },
        );

        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Alice".to_string()));

        sync.track_component_change(1, TypeId::of::<TestComponent>(), "Player".to_string(), data)
            .await
            .unwrap();

        sync.flush().await.unwrap();

        assert_eq!(sync.pending_change_count().await, 0);
        assert_eq!(sync.synced_entity_count().await, 1);
    }

    #[tokio::test]
    async fn test_entity_deletion() {
        let backend = MockBackend::new();
        let mut schema_gen = SchemaGenerator::new();
        schema_gen
            .register::<TestComponent>("Player", vec![("name", SqlType::Text, false)])
            .unwrap();

        let sync = QuerySync::new(backend, schema_gen);

        sync.track_entity_deleted(1).await.unwrap();
        sync.flush().await.unwrap();

        assert_eq!(sync.synced_entity_count().await, 0);
    }

    #[tokio::test]
    async fn test_format_value() {
        let backend = MockBackend::new();
        let schema_gen = SchemaGenerator::new();
        let sync = QuerySync::new(backend, schema_gen);

        assert_eq!(sync.format_value(&Value::Null), "NULL");
        assert_eq!(sync.format_value(&Value::Bool(true)), "TRUE");
        assert_eq!(sync.format_value(&Value::Bool(false)), "FALSE");
        assert_eq!(sync.format_value(&Value::Number(42.into())), "42");
        assert_eq!(sync.format_value(&Value::String("test".to_string())), "'test'");
        assert_eq!(
            sync.format_value(&Value::String("test's".to_string())),
            "'test''s'"
        );
    }
}
