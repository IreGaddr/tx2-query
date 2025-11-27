use tx2_query::sqlite::SqliteBackend;
use tx2_query::backend::DatabaseBackend;

#[tokio::main]
async fn main() {
    let mut backend = SqliteBackend::memory().await.unwrap();

    // Create table
    backend.execute("CREATE TABLE IF NOT EXISTS Player (
        entity_id BIGINT NOT NULL,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        score INTEGER NOT NULL,
        _tx2_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (entity_id)
    )").await.unwrap();

    println!("Table created");

    // Try the INSERT with ON CONFLICT
    let insert_sql = "INSERT INTO Player (entity_id, name, email, score) VALUES (1, 'Alice', 'alice@example.com', 1500) ON CONFLICT (entity_id) DO UPDATE SET name = 'Alice', email = 'alice@example.com', score = 1500";

    println!("Executing: {}", insert_sql);
    backend.execute(insert_sql).await.unwrap();
    println!("Insert succeeded");

    // Try again with same entity_id
    println!("Executing again...");
    backend.execute(insert_sql).await.unwrap();
    println!("Second insert succeeded");

    // Query
    let results = backend.query("SELECT * FROM Player").await.unwrap();
    println!("Results: {} rows", results.len());
}
