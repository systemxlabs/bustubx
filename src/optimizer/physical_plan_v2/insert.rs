use std::sync::Arc;

use crate::catalog::{
    column::{Column, DataType},
    schema::Schema,
};

use super::PhysicalPlanV2;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub input: Arc<PhysicalPlanV2>,
}
impl PhysicalInsert {
    pub fn new(table_name: String, columns: Vec<Column>, input: Arc<PhysicalPlanV2>) -> Self {
        Self {
            table_name,
            columns,
            input,
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
