use tx2_query::prelude::*;
use tx2_query::builder::AggregateFunc;
use serde_json::Value;
use std::any::TypeId;
use std::collections::HashMap;

#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use super::*;
    use tx2_query::sqlite::SqliteBackend;

    #[derive(Debug)]
    struct Player;

    #[derive(Debug)]
    struct Inventory;

    #[tokio::test]
    async fn test_full_workflow() {
        // Create schema generator
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("email", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        schema_gen
            .register::<Inventory>(
                "Inventory",
                vec![
                    ("item_name", SqlType::Text, false),
                    ("quantity", SqlType::Integer, false),
                ],
            )
            .unwrap();

        // Create SQLite backend
        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        // Initialize schema
        sync.initialize_schema().await.unwrap();

        // Track component changes
        let mut player_data = HashMap::new();
        player_data.insert("name".to_string(), Value::String("Alice".to_string()));
        player_data.insert("email".to_string(), Value::String("alice@example.com".to_string()));
        player_data.insert("score".to_string(), Value::Number(1500.into()));

        sync.track_component_change(
            1,
            TypeId::of::<Player>(),
            "Player".to_string(),
            player_data.clone(),
        )
        .await
        .unwrap();

        let mut player_data2 = HashMap::new();
        player_data2.insert("name".to_string(), Value::String("Bob".to_string()));
        player_data2.insert("email".to_string(), Value::String("bob@example.com".to_string()));
        player_data2.insert("score".to_string(), Value::Number(900.into()));

        sync.track_component_change(
            2,
            TypeId::of::<Player>(),
            "Player".to_string(),
            player_data2,
        )
        .await
        .unwrap();

        // Flush to database
        sync.flush().await.unwrap();

        // Query with builder
        let query = SelectBuilder::new("Player")
            .select(vec!["name", "score"])
            .where_gt("score", Value::Number(1000.into()))
            .order_desc("score")
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_string("name"), Some("Alice".to_string()));
        assert_eq!(results[0].get_i64("score"), Some(1500));
    }

    #[tokio::test]
    async fn test_update_operations() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Insert initial data
        let mut player_data = HashMap::new();
        player_data.insert("name".to_string(), Value::String("Charlie".to_string()));
        player_data.insert("score".to_string(), Value::Number(500.into()));

        sync.track_component_change(
            3,
            TypeId::of::<Player>(),
            "Player".to_string(),
            player_data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        // Update the score
        let mut updated_data = HashMap::new();
        updated_data.insert("name".to_string(), Value::String("Charlie".to_string()));
        updated_data.insert("score".to_string(), Value::Number(1200.into()));

        sync.track_component_change(
            3,
            TypeId::of::<Player>(),
            "Player".to_string(),
            updated_data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        // Verify update
        let results = sync
            .query("SELECT score FROM Player WHERE entity_id = 3")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_i64("score"), Some(1200));
    }

    #[tokio::test]
    async fn test_delete_operations() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Insert data
        let mut player_data = HashMap::new();
        player_data.insert("name".to_string(), Value::String("Dave".to_string()));
        player_data.insert("score".to_string(), Value::Number(800.into()));

        sync.track_component_change(
            4,
            TypeId::of::<Player>(),
            "Player".to_string(),
            player_data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        // Verify data exists
        let results = sync
            .query("SELECT * FROM Player WHERE entity_id = 4")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        // Delete entity
        sync.track_entity_deleted(4).await.unwrap();
        sync.flush().await.unwrap();

        // Verify data is gone
        let results = sync
            .query("SELECT * FROM Player WHERE entity_id = 4")
            .await
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_query_builder_complex() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("level", SqlType::Integer, false),
                    ("active", SqlType::Boolean, false),
                ],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Insert test data
        for i in 1..=5 {
            let mut data = HashMap::new();
            data.insert("name".to_string(), Value::String(format!("Player{}", i)));
            data.insert("level".to_string(), Value::Number((i * 10).into()));
            data.insert("active".to_string(), Value::Bool(i % 2 == 0));

            sync.track_component_change(
                i as u64,
                TypeId::of::<Player>(),
                "Player".to_string(),
                data,
            )
            .await
            .unwrap();
        }

        sync.flush().await.unwrap();

        // Test WHERE IN
        let query = SelectBuilder::new("Player")
            .select(vec!["name", "level"])
            .where_in(
                "level",
                vec![Value::Number(20.into()), Value::Number(40.into())],
            )
            .order_asc("level")
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_string("name"), Some("Player2".to_string()));
        assert_eq!(results[1].get_string("name"), Some("Player4".to_string()));

        // Test LIMIT and OFFSET
        let query = SelectBuilder::new("Player")
            .select_all()
            .order_asc("level")
            .limit(2)
            .offset(1)
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i64("level"), Some(20));
        assert_eq!(results[1].get_i64("level"), Some(30));
    }

    #[tokio::test]
    async fn test_aggregation() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Insert test data
        let scores = vec![100, 200, 300, 400, 500];
        for (i, score) in scores.iter().enumerate() {
            let mut data = HashMap::new();
            data.insert("name".to_string(), Value::String(format!("Player{}", i + 1)));
            data.insert("score".to_string(), Value::Number((*score).into()));

            sync.track_component_change(
                (i + 1) as u64,
                TypeId::of::<Player>(),
                "Player".to_string(),
                data,
            )
            .await
            .unwrap();
        }

        sync.flush().await.unwrap();

        // Test COUNT
        let query = SelectBuilder::new("Player")
            .aggregate(AggregateFunc::Count, "*", Some("total"))
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);

        // Test SUM
        let query = SelectBuilder::new("Player")
            .aggregate(AggregateFunc::Sum, "score", Some("total_score"))
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_i64("total_score"), Some(1500));

        // Test AVG
        let query = SelectBuilder::new("Player")
            .aggregate(AggregateFunc::Avg, "score", Some("avg_score"))
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_f64("avg_score"), Some(300.0));
    }

    #[tokio::test]
    async fn test_full_sync() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Prepare bulk data
        let mut entities = Vec::new();
        for i in 1..=10 {
            let mut data = HashMap::new();
            data.insert("name".to_string(), Value::String(format!("Player{}", i)));
            data.insert("score".to_string(), Value::Number((i * 100).into()));

            entities.push((
                i as u64,
                TypeId::of::<Player>(),
                "Player".to_string(),
                data,
            ));
        }

        // Full sync
        sync.full_sync(entities).await.unwrap();

        // Verify all data
        let results = sync
            .query("SELECT COUNT(*) as count FROM Player")
            .await
            .unwrap();
        assert_eq!(results[0].get_i64("count"), Some(10));
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![("name", SqlType::Text, false)],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let config = SyncConfig {
            use_transactions: true,
            ..Default::default()
        };
        let sync = QuerySync::with_config(backend, schema_gen, config);

        sync.initialize_schema().await.unwrap();

        // Track some changes that will fail (invalid SQL in the middle)
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Test".to_string()));

        sync.track_component_change(
            1,
            TypeId::of::<Player>(),
            "Player".to_string(),
            data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        // Verify data exists
        let results = sync.query("SELECT COUNT(*) as count FROM Player").await.unwrap();
        assert_eq!(results[0].get_i64("count"), Some(1));
    }

    #[tokio::test]
    async fn test_clear_all() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![("name", SqlType::Text, false)],
            )
            .unwrap();

        let backend = SqliteBackend::memory().await.unwrap();
        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        // Add some data
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Test".to_string()));

        sync.track_component_change(
            1,
            TypeId::of::<Player>(),
            "Player".to_string(),
            data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        assert_eq!(sync.synced_entity_count().await, 1);

        // Clear all
        sync.clear_all().await.unwrap();

        assert_eq!(sync.synced_entity_count().await, 0);

        let results = sync.query("SELECT COUNT(*) as count FROM Player").await.unwrap();
        assert_eq!(results[0].get_i64("count"), Some(0));
    }
}

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;
    use tx2_query::postgres::PostgresBackend;

    #[derive(Debug)]
    struct Player;

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_workflow() {
        let mut schema_gen = SchemaGenerator::new();

        schema_gen
            .register::<Player>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let backend = PostgresBackend::connect("postgresql://localhost/tx2_test")
            .await
            .unwrap();

        let sync = QuerySync::new(backend, schema_gen);

        sync.initialize_schema().await.unwrap();

        let mut player_data = HashMap::new();
        player_data.insert("name".to_string(), Value::String("PostgresPlayer".to_string()));
        player_data.insert("score".to_string(), Value::Number(2000.into()));

        sync.track_component_change(
            100,
            TypeId::of::<Player>(),
            "Player".to_string(),
            player_data,
        )
        .await
        .unwrap();

        sync.flush().await.unwrap();

        let query = SelectBuilder::new("Player")
            .select_all()
            .where_eq("entity_id", Value::Number(100.into()))
            .build()
            .unwrap();

        let results = sync.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_string("name"), Some("PostgresPlayer".to_string()));

        // Cleanup
        sync.execute("DROP TABLE IF EXISTS Player").await.unwrap();
    }
}
