use crate::buffer::{BufferPoolManager, TABLE_HEAP_BUFFER_POOL_SIZE};
use crate::catalog::{
    Catalog, Column, DataType, Schema, SchemaRef, TableInfo, DEFAULT_CATALOG_NAME,
};
use crate::common::TableReference;
use crate::storage::TableHeap;
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

    pub static ref TABLES_TABLE_REF: TableReference = TableReference::full(
        DEFAULT_CATALOG_NAME.to_string(),
        INFORMATION_SCHEMA_NAME.to_string(),
        INFORMATION_SCHEMA_TABLES.to_string()
    );

    pub static ref COLUMNS_SCHMEA: SchemaRef = Arc::new(Schema::new(vec![
        Column::new("table_catalog".to_string(), DataType::Varchar(None), false),
        Column::new("table_schema".to_string(), DataType::Varchar(None), false),
        Column::new("table_name".to_string(), DataType::Varchar(None), false),
        Column::new("column_name".to_string(), DataType::Varchar(None), false),
        Column::new("data_type".to_string(), DataType::Varchar(None), false),
        Column::new("nullable".to_string(), DataType::Boolean, false),
    ]));

    pub static ref COLUMNS_TABLE_REF: TableReference = TableReference::full(
        DEFAULT_CATALOG_NAME.to_string(),
        INFORMATION_SCHEMA_NAME.to_string(),
        INFORMATION_SCHEMA_COLUMNS.to_string()
    );
}

pub fn load_catalog_data(catalog: &mut Catalog) -> BustubxResult<()> {
    load_information_schema(catalog)?;
    // TODO load tables
    Ok(())
}

fn load_information_schema(catalog: &mut Catalog) -> BustubxResult<()> {
    let meta = catalog
        .buffer_pool_manager
        .disk_manager
        .meta
        .read()
        .unwrap();
    let tables_table = TableInfo {
        schema: TABLES_SCHMEA.clone(),
        name: INFORMATION_SCHEMA_TABLES.to_string(),
        table: TableHeap {
            schema: TABLES_SCHMEA.clone(),
            buffer_pool_manager: BufferPoolManager::new(
                TABLE_HEAP_BUFFER_POOL_SIZE,
                catalog.buffer_pool_manager.disk_manager.clone(),
            ),
            first_page_id: meta.information_schema_tables_first_page_id,
            last_page_id: meta.information_schema_tables_last_page_id,
        },
    };
    catalog
        .tables
        .insert(TABLES_TABLE_REF.extend_to_full(), tables_table);

    let columns_table = TableInfo {
        schema: COLUMNS_SCHMEA.clone(),
        name: INFORMATION_SCHEMA_COLUMNS.to_string(),
        table: TableHeap {
            schema: COLUMNS_SCHMEA.clone(),
            buffer_pool_manager: BufferPoolManager::new(
                TABLE_HEAP_BUFFER_POOL_SIZE,
                catalog.buffer_pool_manager.disk_manager.clone(),
            ),
            first_page_id: meta.information_schema_columns_first_page_id,
            last_page_id: meta.information_schema_columns_last_page_id,
        },
    };
    catalog
        .tables
        .insert(COLUMNS_TABLE_REF.extend_to_full(), columns_table);
    Ok(())
}
