use crate::catalog::schema::{self, Schema};

#[derive(Debug, Clone)]
pub struct LogicalCreateTableOperator {
    pub table_name: String,
    pub schema: Schema,
}
impl LogicalCreateTableOperator {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }
}
