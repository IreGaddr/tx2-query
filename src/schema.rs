use crate::error::{QueryError, Result};
use std::any::TypeId;
use std::collections::HashMap;

/// SQL type mapping
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    BigInt,
    Integer,
    SmallInt,
    Real,
    DoublePrecision,
    Text,
    Boolean,
    Timestamp,
    Json,
    Bytea,
}

impl SqlType {
    pub fn to_sql(&self) -> &'static str {
        match self {
            SqlType::BigInt => "BIGINT",
            SqlType::Integer => "INTEGER",
            SqlType::SmallInt => "SMALLINT",
            SqlType::Real => "REAL",
            SqlType::DoublePrecision => "DOUBLE PRECISION",
            SqlType::Text => "TEXT",
            SqlType::Boolean => "BOOLEAN",
            SqlType::Timestamp => "TIMESTAMP",
            SqlType::Json => "JSONB",
            SqlType::Bytea => "BYTEA",
        }
    }
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub default: Option<String>,
}

/// Table schema
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
}

/// Index definition
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// Component registration
pub struct ComponentRegistration {
    pub type_id: TypeId,
    pub name: String,
    pub schema: TableSchema,
}

/// Schema generator
pub struct SchemaGenerator {
    registrations: HashMap<TypeId, ComponentRegistration>,
    component_names: HashMap<String, TypeId>,
}

impl SchemaGenerator {
    pub fn new() -> Self {
        Self {
            registrations: HashMap::new(),
            component_names: HashMap::new(),
        }
    }

    /// Register a component type
    pub fn register<T>(&mut self, name: &str, fields: Vec<(&str, SqlType, bool)>) -> Result<()>
    where
        T: 'static,
    {
        let type_id = TypeId::of::<T>();

        if self.registrations.contains_key(&type_id) {
            return Err(QueryError::Schema(format!(
                "Component {} already registered",
                name
            )));
        }

        let mut columns = vec![
            ColumnDef {
                name: "entity_id".to_string(),
                sql_type: SqlType::BigInt,
                nullable: false,
                default: None,
            },
        ];

        for (field_name, field_type, nullable) in fields {
            columns.push(ColumnDef {
                name: field_name.to_string(),
                sql_type: field_type,
                nullable,
                default: None,
            });
        }

        // Add metadata column
        columns.push(ColumnDef {
            name: "_tx2_updated_at".to_string(),
            sql_type: SqlType::Timestamp,
            nullable: false,
            default: Some("CURRENT_TIMESTAMP".to_string()),
        });

        let schema = TableSchema {
            name: name.to_string(),
            columns,
            indexes: vec![],
        };

        let registration = ComponentRegistration {
            type_id,
            name: name.to_string(),
            schema,
        };

        self.component_names.insert(name.to_string(), type_id);
        self.registrations.insert(type_id, registration);

        Ok(())
    }

    /// Get schema for a component
    pub fn get_schema(&self, type_id: &TypeId) -> Option<&TableSchema> {
        self.registrations.get(type_id).map(|r| &r.schema)
    }

    /// Get schema by component name
    pub fn get_schema_by_name(&self, name: &str) -> Option<&TableSchema> {
        self.component_names
            .get(name)
            .and_then(|type_id| self.get_schema(type_id))
    }

    /// Generate CREATE TABLE SQL for all registered components
    pub fn generate_ddl(&self) -> String {
        let mut ddl = String::new();

        for registration in self.registrations.values() {
            ddl.push_str(&self.generate_table_ddl(&registration.schema));
            ddl.push_str("\n\n");
        }

        ddl.trim().to_string()
    }

    /// Generate CREATE TABLE SQL for a single table
    pub fn generate_table_ddl(&self, schema: &TableSchema) -> String {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (\n", schema.name);

        let column_defs: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let mut def = format!("    {} {}", col.name, col.sql_type.to_sql());

                if !col.nullable {
                    def.push_str(" NOT NULL");
                }

                if let Some(default) = &col.default {
                    def.push_str(&format!(" DEFAULT {}", default));
                }

                def
            })
            .collect();

        sql.push_str(&column_defs.join(",\n"));
        sql.push_str(",\n    PRIMARY KEY (entity_id)\n");
        sql.push_str(");");

        // Add indexes
        for index in &schema.indexes {
            sql.push_str("\n\n");
            sql.push_str(&self.generate_index_ddl(&schema.name, index));
        }

        sql
    }

    /// Generate CREATE INDEX SQL
    pub fn generate_index_ddl(&self, table_name: &str, index: &IndexDef) -> String {
        let unique = if index.unique { "UNIQUE " } else { "" };
        format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} ({});",
            unique,
            index.name,
            table_name,
            index.columns.join(", ")
        )
    }

    /// Add an index to a component schema
    pub fn add_index(
        &mut self,
        type_id: &TypeId,
        index_name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<()> {
        let registration = self
            .registrations
            .get_mut(type_id)
            .ok_or_else(|| QueryError::ComponentNotRegistered(format!("{:?}", type_id)))?;

        registration.schema.indexes.push(IndexDef {
            name: index_name.to_string(),
            columns,
            unique,
        });

        Ok(())
    }

    /// List all registered component names
    pub fn list_components(&self) -> Vec<String> {
        self.component_names.keys().cloned().collect()
    }
}

impl Default for SchemaGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestComponent;

    #[test]
    fn test_schema_generation() {
        let mut generator = SchemaGenerator::new();

        generator
            .register::<TestComponent>(
                "Player",
                vec![
                    ("name", SqlType::Text, false),
                    ("email", SqlType::Text, false),
                    ("score", SqlType::Integer, false),
                ],
            )
            .unwrap();

        let ddl = generator.generate_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS Player"));
        assert!(ddl.contains("entity_id BIGINT NOT NULL"));
        assert!(ddl.contains("name TEXT NOT NULL"));
        assert!(ddl.contains("email TEXT NOT NULL"));
        assert!(ddl.contains("score INTEGER NOT NULL"));
        assert!(ddl.contains("_tx2_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(ddl.contains("PRIMARY KEY (entity_id)"));
    }

    #[test]
    fn test_index_generation() {
        let mut generator = SchemaGenerator::new();

        generator
            .register::<TestComponent>(
                "Player",
                vec![("name", SqlType::Text, false)],
            )
            .unwrap();

        let type_id = TypeId::of::<TestComponent>();
        generator
            .add_index(&type_id, "idx_player_name", vec!["name".to_string()], false)
            .unwrap();

        let schema = generator.get_schema(&type_id).unwrap();
        let index_ddl = generator.generate_index_ddl(&schema.name, &schema.indexes[0]);

        assert!(index_ddl.contains("CREATE INDEX IF NOT EXISTS idx_player_name"));
        assert!(index_ddl.contains("ON Player (name)"));
    }
}
