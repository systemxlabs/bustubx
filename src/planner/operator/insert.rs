use crate::catalog::{column::Column, schema::Schema};

#[derive(Debug, Clone)]
pub struct LogicalInsertOperator {
    pub table_name: String,
    pub columns: Vec<Column>,
}
impl LogicalInsertOperator {
    pub fn new(table_name: String, columns: Vec<Column>) -> Self {
        Self {
            table_name,
            columns,
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(self.columns.clone())
    }
}
