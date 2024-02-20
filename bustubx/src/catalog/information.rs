use crate::catalog::{Catalog, Column, DataType, Schema, SchemaRef};
use crate::BustubxResult;
use std::sync::Arc;

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub static INFORMATION_SCHEMA_TABLES: &str = "tables";
pub static INFORMATION_SCHEMA_COLUMNS: &str = "columns";

lazy_static::lazy_static! {
    pub static ref TABLES_SCHMEA: SchemaRef = Arc::new(Schema::new(vec![
        Column::new("table_catalog".to_string(), DataType::Varchar(None), false),
        Column::new("table_schema".to_string(), DataType::Varchar(None), false),
        Column::new("table_name".to_string(), DataType::Varchar(None), false),
        Column::new("first_page_id".to_string(), DataType::UInt64, false),
        Column::new("last_page_id".to_string(), DataType::UInt64, false),
    ]));

    pub static ref COLUMNS_SCHMEA: SchemaRef = Arc::new(Schema::new(vec![
        Column::new("table_catalog".to_string(), DataType::Varchar(None), false),
        Column::new("table_schema".to_string(), DataType::Varchar(None), false),
        Column::new("table_name".to_string(), DataType::Varchar(None), false),
        Column::new("column_name".to_string(), DataType::Varchar(None), false),
        Column::new("data_type".to_string(), DataType::Varchar(None), false),
        Column::new("nullable".to_string(), DataType::Boolean, false),
    ]));
}

pub fn load_catalog_data(catalog: &mut Catalog) -> BustubxResult<()> {
    // TODO load information_schema
    Ok(())
}
