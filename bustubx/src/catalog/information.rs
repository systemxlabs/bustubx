use crate::catalog::{Column, DataType, Schema, SchemaRef};
use std::sync::Arc;

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub static INFORMATION_SCHEMA_TABLES: &str = "tables";
pub static INFORMATION_SCHEMA_COLUMNS: &str = "columns";

lazy_static::lazy_static! {
    pub static ref TABLES_SCHMEA: SchemaRef = Arc::new(Schema::new(vec![
        // TODO need varchar support
        Column::new("table_catalog".to_string(), DataType::Int8, false),
    ]));
}
