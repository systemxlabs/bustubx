use crate::catalog::{
    column::{Column, DataType},
    schema::Schema,
};

#[derive(Debug)]
pub struct PhysicalInsertOperator {
    pub table_name: String,
    pub columns: Vec<Column>,
}
impl PhysicalInsertOperator {
    pub fn new(table_name: String, columns: Vec<Column>) -> Self {
        Self {
            table_name,
            columns,
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(vec![Column::new(
            None,
            "insert_rows".to_string(),
            DataType::Integer,
            0,
        )])
    }
}
