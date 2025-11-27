use crate::error::{QueryError, Result};
use serde_json::Value;
use std::fmt;

/// Comparison operators for WHERE clauses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "="),
            ComparisonOp::Ne => write!(f, "!="),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::Le => write!(f, "<="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Ge => write!(f, ">="),
            ComparisonOp::Like => write!(f, "LIKE"),
            ComparisonOp::NotLike => write!(f, "NOT LIKE"),
            ComparisonOp::In => write!(f, "IN"),
            ComparisonOp::NotIn => write!(f, "NOT IN"),
            ComparisonOp::IsNull => write!(f, "IS NULL"),
            ComparisonOp::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// Logical operators for combining conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

impl fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOp::And => write!(f, "AND"),
            LogicalOp::Or => write!(f, "OR"),
        }
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl fmt::Display for SortDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "ASC"),
            SortDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
        }
    }
}

/// Aggregate function
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunc {
    pub fn to_sql(&self, column: &str) -> String {
        match self {
            AggregateFunc::Count => "COUNT(*)".to_string(),
            AggregateFunc::CountDistinct => format!("COUNT(DISTINCT {})", column),
            AggregateFunc::Sum => format!("SUM({})", column),
            AggregateFunc::Avg => format!("AVG({})", column),
            AggregateFunc::Min => format!("MIN({})", column),
            AggregateFunc::Max => format!("MAX({})", column),
        }
    }
}

/// WHERE condition
#[derive(Debug, Clone)]
pub enum Condition {
    Simple {
        column: String,
        op: ComparisonOp,
        value: Option<Value>,
    },
    Compound {
        conditions: Vec<Condition>,
        op: LogicalOp,
    },
    Raw(String),
}

impl Condition {
    pub fn to_sql(&self) -> String {
        match self {
            Condition::Simple { column, op, value } => {
                if matches!(op, ComparisonOp::IsNull | ComparisonOp::IsNotNull) {
                    format!("{} {}", column, op)
                } else if matches!(op, ComparisonOp::In | ComparisonOp::NotIn) {
                    if let Some(Value::Array(arr)) = value {
                        let values = arr
                            .iter()
                            .map(format_value)
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{} {} ({})", column, op, values)
                    } else {
                        format!("{} {} ()", column, op)
                    }
                } else {
                    let val = value
                        .as_ref()
                        .map(format_value)
                        .unwrap_or_else(|| "NULL".to_string());
                    format!("{} {} {}", column, op, val)
                }
            }
            Condition::Compound { conditions, op } => {
                if conditions.is_empty() {
                    "TRUE".to_string()
                } else {
                    let parts = conditions
                        .iter()
                        .map(|c| c.to_sql())
                        .collect::<Vec<_>>()
                        .join(&format!(" {} ", op));
                    format!("({})", parts)
                }
            }
            Condition::Raw(sql) => sql.clone(),
        }
    }
}

/// Join clause
#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: JoinType,
    pub table: String,
    pub on_condition: Condition,
}

impl Join {
    pub fn to_sql(&self) -> String {
        format!("{} {} ON {}", self.join_type, self.table, self.on_condition.to_sql())
    }
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
}

impl OrderBy {
    pub fn to_sql(&self) -> String {
        format!("{} {}", self.column, self.direction)
    }
}

/// SELECT query builder
#[derive(Debug, Clone)]
pub struct SelectBuilder {
    table: String,
    columns: Vec<String>,
    joins: Vec<Join>,
    where_clause: Option<Condition>,
    group_by: Vec<String>,
    having: Option<Condition>,
    order_by: Vec<OrderBy>,
    limit: Option<usize>,
    offset: Option<usize>,
    distinct: bool,
}

impl SelectBuilder {
    /// Create a new SELECT query builder
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: vec!["*".to_string()],
            joins: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
        }
    }

    /// Select specific columns
    pub fn select(mut self, columns: Vec<impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(|c| c.into()).collect();
        self
    }

    /// Select all columns
    pub fn select_all(mut self) -> Self {
        self.columns = vec!["*".to_string()];
        self
    }

    /// Add a column to select
    pub fn add_column(mut self, column: impl Into<String>) -> Self {
        if self.columns == vec!["*".to_string()] {
            self.columns.clear();
        }
        self.columns.push(column.into());
        self
    }

    /// Add an aggregate function
    pub fn aggregate(mut self, func: AggregateFunc, column: impl Into<String>, alias: Option<impl Into<String>>) -> Self {
        if self.columns == vec!["*".to_string()] {
            self.columns.clear();
        }
        let col_str = func.to_sql(&column.into());
        if let Some(alias) = alias {
            self.columns.push(format!("{} AS {}", col_str, alias.into()));
        } else {
            self.columns.push(col_str);
        }
        self
    }

    /// Use DISTINCT
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a WHERE condition
    pub fn where_clause(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }

    /// Add an AND condition to existing WHERE
    pub fn and_where(mut self, condition: Condition) -> Self {
        if let Some(existing) = self.where_clause {
            self.where_clause = Some(Condition::Compound {
                conditions: vec![existing, condition],
                op: LogicalOp::And,
            });
        } else {
            self.where_clause = Some(condition);
        }
        self
    }

    /// Add an OR condition to existing WHERE
    pub fn or_where(mut self, condition: Condition) -> Self {
        if let Some(existing) = self.where_clause {
            self.where_clause = Some(Condition::Compound {
                conditions: vec![existing, condition],
                op: LogicalOp::Or,
            });
        } else {
            self.where_clause = Some(condition);
        }
        self
    }

    /// Add a simple WHERE condition (column = value)
    pub fn where_eq(self, column: impl Into<String>, value: Value) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Eq,
            value: Some(value),
        })
    }

    /// Add a WHERE column > value condition
    pub fn where_gt(self, column: impl Into<String>, value: Value) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Gt,
            value: Some(value),
        })
    }

    /// Add a WHERE column < value condition
    pub fn where_lt(self, column: impl Into<String>, value: Value) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Lt,
            value: Some(value),
        })
    }

    /// Add a WHERE column IN (...) condition
    pub fn where_in(self, column: impl Into<String>, values: Vec<Value>) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::In,
            value: Some(Value::Array(values)),
        })
    }

    /// Add a WHERE column IS NULL condition
    pub fn where_null(self, column: impl Into<String>) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::IsNull,
            value: None,
        })
    }

    /// Add a WHERE column LIKE pattern condition
    pub fn where_like(self, column: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.and_where(Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Like,
            value: Some(Value::String(pattern.into())),
        })
    }

    /// Add a JOIN clause
    pub fn join(mut self, join_type: JoinType, table: impl Into<String>, on: Condition) -> Self {
        self.joins.push(Join {
            join_type,
            table: table.into(),
            on_condition: on,
        });
        self
    }

    /// Add an INNER JOIN
    pub fn inner_join(self, table: impl Into<String>, on: Condition) -> Self {
        self.join(JoinType::Inner, table, on)
    }

    /// Add a LEFT JOIN
    pub fn left_join(self, table: impl Into<String>, on: Condition) -> Self {
        self.join(JoinType::Left, table, on)
    }

    /// Add GROUP BY columns
    pub fn group_by(mut self, columns: Vec<impl Into<String>>) -> Self {
        self.group_by = columns.into_iter().map(|c| c.into()).collect();
        self
    }

    /// Add a HAVING condition
    pub fn having(mut self, condition: Condition) -> Self {
        self.having = Some(condition);
        self
    }

    /// Add ORDER BY
    pub fn order_by(mut self, column: impl Into<String>, direction: SortDirection) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    /// Add ascending ORDER BY
    pub fn order_asc(self, column: impl Into<String>) -> Self {
        self.order_by(column, SortDirection::Asc)
    }

    /// Add descending ORDER BY
    pub fn order_desc(self, column: impl Into<String>) -> Self {
        self.order_by(column, SortDirection::Desc)
    }

    /// Set LIMIT
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set OFFSET
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Build the SQL query string
    pub fn build(self) -> Result<String> {
        let mut sql = String::from("SELECT ");

        if self.distinct {
            sql.push_str("DISTINCT ");
        }

        sql.push_str(&self.columns.join(", "));
        sql.push_str(&format!(" FROM {}", self.table));

        for join in &self.joins {
            sql.push_str(" ");
            sql.push_str(&join.to_sql());
        }

        if let Some(where_clause) = &self.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.to_sql());
        }

        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        if let Some(having) = &self.having {
            sql.push_str(" HAVING ");
            sql.push_str(&having.to_sql());
        }

        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &self
                    .order_by
                    .iter()
                    .map(|o| o.to_sql())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        Ok(sql)
    }

    /// Build and return the SQL query string (convenience method)
    pub fn to_sql(self) -> Result<String> {
        self.build()
    }
}

/// UPDATE query builder
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    table: String,
    set_values: Vec<(String, Value)>,
    where_clause: Option<Condition>,
}

impl UpdateBuilder {
    /// Create a new UPDATE query builder
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            set_values: Vec::new(),
            where_clause: None,
        }
    }

    /// Set a column value
    pub fn set(mut self, column: impl Into<String>, value: Value) -> Self {
        self.set_values.push((column.into(), value));
        self
    }

    /// Set multiple column values
    pub fn set_many(mut self, values: Vec<(impl Into<String>, Value)>) -> Self {
        for (col, val) in values {
            self.set_values.push((col.into(), val));
        }
        self
    }

    /// Add WHERE condition
    pub fn where_clause(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }

    /// Add simple WHERE condition (column = value)
    pub fn where_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        let condition = Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Eq,
            value: Some(value),
        };
        if let Some(existing) = self.where_clause {
            self.where_clause = Some(Condition::Compound {
                conditions: vec![existing, condition],
                op: LogicalOp::And,
            });
        } else {
            self.where_clause = Some(condition);
        }
        self
    }

    /// Build the SQL query string
    pub fn build(self) -> Result<String> {
        if self.set_values.is_empty() {
            return Err(QueryError::Query("UPDATE must have at least one SET value".to_string()));
        }

        let mut sql = format!("UPDATE {} SET ", self.table);

        let set_clauses: Vec<String> = self
            .set_values
            .iter()
            .map(|(col, val)| format!("{} = {}", col, format_value(val)))
            .collect();

        sql.push_str(&set_clauses.join(", "));

        if let Some(where_clause) = &self.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.to_sql());
        }

        Ok(sql)
    }

    /// Build and return the SQL query string (convenience method)
    pub fn to_sql(self) -> Result<String> {
        self.build()
    }
}

/// DELETE query builder
#[derive(Debug, Clone)]
pub struct DeleteBuilder {
    table: String,
    where_clause: Option<Condition>,
}

impl DeleteBuilder {
    /// Create a new DELETE query builder
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            where_clause: None,
        }
    }

    /// Add WHERE condition
    pub fn where_clause(mut self, condition: Condition) -> Self {
        self.where_clause = Some(condition);
        self
    }

    /// Add simple WHERE condition (column = value)
    pub fn where_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        let condition = Condition::Simple {
            column: column.into(),
            op: ComparisonOp::Eq,
            value: Some(value),
        };
        if let Some(existing) = self.where_clause {
            self.where_clause = Some(Condition::Compound {
                conditions: vec![existing, condition],
                op: LogicalOp::And,
            });
        } else {
            self.where_clause = Some(condition);
        }
        self
    }

    /// Build the SQL query string
    pub fn build(self) -> Result<String> {
        let mut sql = format!("DELETE FROM {}", self.table);

        if let Some(where_clause) = &self.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.to_sql());
        }

        Ok(sql)
    }

    /// Build and return the SQL query string (convenience method)
    pub fn to_sql(self) -> Result<String> {
        self.build()
    }
}

/// Format a JSON value for SQL
fn format_value(value: &Value) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_basic() {
        let query = SelectBuilder::new("users")
            .select_all()
            .build()
            .unwrap();

        assert_eq!(query, "SELECT * FROM users");
    }

    #[test]
    fn test_select_columns() {
        let query = SelectBuilder::new("users")
            .select(vec!["id", "name", "email"])
            .build()
            .unwrap();

        assert_eq!(query, "SELECT id, name, email FROM users");
    }

    #[test]
    fn test_select_where() {
        let query = SelectBuilder::new("users")
            .select_all()
            .where_eq("id", Value::Number(1.into()))
            .build()
            .unwrap();

        assert_eq!(query, "SELECT * FROM users WHERE id = 1");
    }

    #[test]
    fn test_select_where_multiple() {
        let query = SelectBuilder::new("users")
            .select_all()
            .where_eq("age", Value::Number(25.into()))
            .where_gt("score", Value::Number(100.into()))
            .build()
            .unwrap();

        assert_eq!(query, "SELECT * FROM users WHERE (age = 25 AND score > 100)");
    }

    #[test]
    fn test_select_where_in() {
        let query = SelectBuilder::new("users")
            .select_all()
            .where_in("id", vec![Value::Number(1.into()), Value::Number(2.into())])
            .build()
            .unwrap();

        assert_eq!(query, "SELECT * FROM users WHERE id IN (1, 2)");
    }

    #[test]
    fn test_select_join() {
        let query = SelectBuilder::new("users")
            .select(vec!["users.name", "posts.title"])
            .inner_join(
                "posts",
                Condition::Raw("users.id = posts.user_id".to_string()),
            )
            .build()
            .unwrap();

        assert_eq!(
            query,
            "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id"
        );
    }

    #[test]
    fn test_select_order_limit() {
        let query = SelectBuilder::new("users")
            .select_all()
            .order_desc("created_at")
            .limit(10)
            .build()
            .unwrap();

        assert_eq!(query, "SELECT * FROM users ORDER BY created_at DESC LIMIT 10");
    }

    #[test]
    fn test_select_group_by() {
        let query = SelectBuilder::new("orders")
            .add_column("user_id")
            .aggregate(AggregateFunc::Count, "*", Some("order_count"))
            .group_by(vec!["user_id"])
            .build()
            .unwrap();

        assert_eq!(query, "SELECT user_id, COUNT(*) AS order_count FROM orders GROUP BY user_id");
    }

    #[test]
    fn test_select_distinct() {
        let query = SelectBuilder::new("users")
            .select(vec!["country"])
            .distinct()
            .build()
            .unwrap();

        assert_eq!(query, "SELECT DISTINCT country FROM users");
    }

    #[test]
    fn test_update_basic() {
        let query = UpdateBuilder::new("users")
            .set("name", Value::String("Alice".to_string()))
            .where_eq("id", Value::Number(1.into()))
            .build()
            .unwrap();

        assert_eq!(query, "UPDATE users SET name = 'Alice' WHERE id = 1");
    }

    #[test]
    fn test_update_multiple() {
        let query = UpdateBuilder::new("users")
            .set("name", Value::String("Alice".to_string()))
            .set("age", Value::Number(30.into()))
            .where_eq("id", Value::Number(1.into()))
            .build()
            .unwrap();

        assert_eq!(query, "UPDATE users SET name = 'Alice', age = 30 WHERE id = 1");
    }

    #[test]
    fn test_delete_basic() {
        let query = DeleteBuilder::new("users")
            .where_eq("id", Value::Number(1.into()))
            .build()
            .unwrap();

        assert_eq!(query, "DELETE FROM users WHERE id = 1");
    }

    #[test]
    fn test_condition_is_null() {
        let condition = Condition::Simple {
            column: "deleted_at".to_string(),
            op: ComparisonOp::IsNull,
            value: None,
        };

        assert_eq!(condition.to_sql(), "deleted_at IS NULL");
    }

    #[test]
    fn test_condition_like() {
        let condition = Condition::Simple {
            column: "name".to_string(),
            op: ComparisonOp::Like,
            value: Some(Value::String("%Alice%".to_string())),
        };

        assert_eq!(condition.to_sql(), "name LIKE '%Alice%'");
    }

    #[test]
    fn test_format_value_string_escaping() {
        let value = Value::String("O'Reilly".to_string());
        assert_eq!(format_value(&value), "'O''Reilly'");
    }
}
