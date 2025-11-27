# tx2-query

SQL Analytics Layer for TX-2 ECS - enabling advanced analytics, complex queries, and external tool integration while keeping the ECS world as the source of truth.

## Overview

tx2-query provides a SQL side-car database for TX-2 applications. It's designed as an **analytics layer, NOT an ORM**:

- **ECS World → SQL Database**: One-way synchronization from ECS to SQL
- **Component → Table Mapping**: Automatic schema generation from component types
- **Incremental Sync**: Track and sync only changed entities/components
- **Multiple Backends**: PostgreSQL, SQLite, and extensible to more

## Philosophy

- ECS world is always the source of truth
- SQL database is a read-only projection for analytics
- No bi-directional sync - changes flow ECS → SQL only
- Optimized for analytical queries, not transactional workloads

## Features

- **Schema Generation**: Automatic DDL creation from component definitions
- **Query Builder**: Ergonomic SQL query construction
- **Sync API**: Incremental synchronization with batching
- **Backend Abstraction**: Support for multiple SQL databases
- **Type-Safe**: Leverages Rust's type system for safety
- **DuckDB Support**: OLAP workloads with columnar storage and window functions

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tx2-query = { version = "0.1", features = ["postgres", "sqlite", "duckdb"] }
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

```rust
use tx2_query::prelude::*;
use serde_json::json;

#[derive(Debug)]
struct Player;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create schema generator
    let mut schema_gen = SchemaGenerator::new();
    schema_gen.register::<Player>(
        "Player",
        vec![
            ("name", SqlType::Text, false),
            ("email", SqlType::Text, false),
            ("score", SqlType::Integer, false),
        ],
    )?;

    // 2. Connect to database
    let backend = SqliteBackend::memory().await?;
    let sync = QuerySync::new(backend, schema_gen);

    // 3. Initialize schema
    sync.initialize_schema().await?;

    // 4. Track changes from your ECS world
    let mut player_data = HashMap::new();
    player_data.insert("name".to_string(), json!("Alice"));
    player_data.insert("email".to_string(), json!("alice@example.com"));
    player_data.insert("score".to_string(), json!(1500));

    sync.track_component_change(
        1,  // entity_id
        TypeId::of::<Player>(),
        "Player".to_string(),
        player_data,
    ).await?;

    // 5. Flush to database
    sync.flush().await?;

    // 6. Query with builder
    let query = SelectBuilder::new("Player")
        .select(vec!["name", "score"])
        .where_gt("score", json!(1000))
        .order_desc("score")
        .limit(10)
        .build()?;

    let results = sync.query(&query).await?;
    for row in results {
        println!("{}: {}",
            row.get_string("name").unwrap(),
            row.get_i64("score").unwrap()
        );
    }

    Ok(())
}
```

## Database Backends

### SQLite

```rust
use tx2_query::sqlite::SqliteBackend;

// In-memory database
let backend = SqliteBackend::memory().await?;

// File-based database
let backend = SqliteBackend::file("mydb.sqlite").await?;
```

### PostgreSQL

```rust
use tx2_query::postgres::PostgresBackend;

let backend = PostgresBackend::connect(
    "postgresql://user:password@localhost/database"
).await?;
```

### DuckDB

```rust
use tx2_query::duckdb::DuckDBBackend;

// In-memory database (perfect for analytics)
let backend = DuckDBBackend::memory().await?;

// File-based database
let backend = DuckDBBackend::file("analytics.duckdb").await?;

// Export to Parquet for external analysis
backend.export_parquet("Player", "players.parquet").await?;

// Import from Parquet
backend.import_parquet("Player", "players.parquet").await?;
```

## Query Builder

The query builder provides an ergonomic, type-safe way to construct SQL queries:

```rust
// SELECT with WHERE
let query = SelectBuilder::new("Player")
    .select(vec!["name", "score"])
    .where_eq("active", json!(true))
    .where_gt("score", json!(100))
    .build()?;

// Aggregations
let query = SelectBuilder::new("Player")
    .aggregate(AggregateFunc::Count, "*", Some("total"))
    .aggregate(AggregateFunc::Avg, "score", Some("avg_score"))
    .build()?;

// Ordering and pagination
let query = SelectBuilder::new("Player")
    .select_all()
    .order_desc("created_at")
    .limit(20)
    .offset(40)
    .build()?;

// Complex conditions
let query = SelectBuilder::new("Player")
    .select_all()
    .where_in("level", vec![json!(10), json!(20), json!(30)])
    .where_like("name", "%Alice%")
    .build()?;
```

## Schema Generation

Components automatically map to SQL tables:

```rust
#[derive(Debug)]
struct Player;

schema_gen.register::<Player>(
    "Player",  // Table name
    vec![
        ("name", SqlType::Text, false),      // NOT NULL
        ("score", SqlType::Integer, false),
        ("notes", SqlType::Text, true),       // NULL able
    ],
)?;
```

Generated DDL:

```sql
CREATE TABLE IF NOT EXISTS Player (
    entity_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    score INTEGER NOT NULL,
    notes TEXT,
    _tx2_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_id)
);
```

## Synchronization

### Incremental Sync

Track changes as they happen in your ECS world:

```rust
// Entity created
sync.track_entity_created(entity_id).await?;

// Component added/modified
sync.track_component_change(
    entity_id,
    component_type_id,
    component_name,
    data,
).await?;

// Entity deleted
sync.track_entity_deleted(entity_id).await?;

// Flush changes to database
sync.flush().await?;
```

### Full Sync

Perform a full synchronization of all entities:

```rust
let entities = vec![
    (entity_id, type_id, "Player".to_string(), player_data),
    // ... more entities
];

sync.full_sync(entities).await?;
```

### Configuration

```rust
let config = SyncConfig {
    batch_size: 1000,              // Batch size for bulk operations
    background_sync: false,         // Sync in background
    use_transactions: false,        // Use transactions (see note below)
    max_retries: 3,                // Max retries on failure
};

let sync = QuerySync::with_config(backend, schema_gen, config);
```

**Note on Transactions**: Transaction support is limited when using connection pooling. For production use with proper transaction isolation, consider implementing connection affinity or using a dedicated transaction connection.

## SQL Types

Supported SQL types:

- `SqlType::BigInt` - 64-bit integer
- `SqlType::Integer` - 32-bit integer
- `SqlType::SmallInt` - 16-bit integer
- `SqlType::Real` - Single precision float
- `SqlType::DoublePrecision` - Double precision float
- `SqlType::Text` - Variable length text
- `SqlType::Boolean` - Boolean
- `SqlType::Timestamp` - Timestamp
- `SqlType::Json` - JSON/JSONB data
- `SqlType::Bytea` - Binary data

## Why DuckDB for Analytics?

DuckDB is optimized for OLAP workloads and provides several advantages:

- **Columnar Storage**: Efficient storage and query performance for analytics
- **Window Functions**: Advanced analytics with `ROW_NUMBER()`, `RANK()`, `LAG()`, etc.
- **Parquet Integration**: Export/import data in industry-standard format
- **No Server Required**: Embedded database, no separate process
- **Fast Aggregations**: Optimized for `GROUP BY`, `SUM()`, `AVG()`, etc.
- **Join Performance**: Excellent performance on complex joins

Example analytical query:

```rust
use tx2_query::duckdb::DuckDBBackend;
use tx2_query::prelude::*;

let mut backend = DuckDBBackend::memory().await?;
let sync = QuerySync::new(backend, schema_gen);

// Complex analytical query with window functions
let results = sync.query("
    SELECT
        player_name,
        score,
        AVG(score) OVER (PARTITION BY level) as avg_level_score,
        RANK() OVER (ORDER BY score DESC) as global_rank
    FROM Player
    WHERE active = true
").await?;
```

## Use Cases

### Analytics Dashboard

```rust
// Get player statistics
let query = SelectBuilder::new("Player")
    .aggregate(AggregateFunc::Count, "*", Some("total_players"))
    .aggregate(AggregateFunc::Avg, "score", Some("avg_score"))
    .aggregate(AggregateFunc::Max, "score", Some("high_score"))
    .build()?;
```

### Leaderboards

```rust
// Top 10 players
let query = SelectBuilder::new("Player")
    .select(vec!["name", "score", "level"])
    .where_eq("active", json!(true))
    .order_desc("score")
    .limit(10)
    .build()?;
```

### External Tools

Connect BI tools, data visualization platforms, or SQL clients directly to the SQL database for analysis without affecting your ECS world.

## Performance Tips

1. **Batch Operations**: Use appropriate `batch_size` to balance memory and performance
2. **Indexes**: Add indexes for frequently queried columns
3. **Selective Sync**: Only sync components needed for analytics
4. **Background Sync**: Enable background sync for non-blocking updates
5. **Clear Unused Data**: Periodically use `clear_all()` if full re-sync is needed

## Testing

```bash
# Run all tests
cargo test

# Run with specific backend
cargo test --features sqlite
cargo test --features postgres

# Run integration tests (requires database)
cargo test --test integration_test
```

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or PR on GitHub.

## Roadmap

- [x] DuckDB backend for OLAP workloads
- [ ] ClickHouse backend for time-series analytics
- [ ] Historical queries via tx2-pack integration
- [ ] Materialized views support
- [ ] Query result caching
- [ ] Automatic index recommendations

## Related Crates

- **tx2-core**: Core ECS framework
- **tx2-link**: Serialization and messaging
- **tx2-pack**: Delta compression and time travel
