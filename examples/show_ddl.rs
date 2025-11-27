use tx2_query::schema::{SchemaGenerator, SqlType};
use std::any::TypeId;

#[derive(Debug)]
struct Player;

#[derive(Debug)]
struct Inventory;

fn main() {
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

    let ddl = schema_gen.generate_ddl();
    println!("Generated DDL:");
    println!("{}", ddl);
    println!("\nSplit by semicolon:");
    for (i, statement) in ddl.split(";").enumerate() {
        println!("Statement {}:", i);
        println!("{}", statement.trim());
        println!("---");
    }
}
