use crate::catalog::{column::Column, schema::Schema};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalInsertOperator {
    pub table_name: String,
    pub columns: Vec<Column>,
}
