use crate::buffer::{AtomicPageId, PageId, INVALID_PAGE_ID};
use crate::catalog::{Catalog, Column, DataType, Schema, SchemaRef, DEFAULT_CATALOG_NAME};
use crate::common::{ScalarValue, TableReference};
use crate::storage::codec::TablePageCodec;
use crate::storage::TableHeap;
use crate::{BustubxError, BustubxResult, Database};
use std::sync::Arc;

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub static INFORMATION_SCHEMA_TABLES: &str = "tables";
pub static INFORMATION_SCHEMA_COLUMNS: &str = "columns";

lazy_static::lazy_static! {
    pub static ref TABLES_SCHMEA: SchemaRef = Arc::new(Schema::new(vec![
        Column::new("table_catalog".to_string(), DataType::Varchar(None), false),
        Column::new("table_schema".to_string(), DataType::Varchar(None), false),
        Column::new("table_name".to_string(), DataType::Varchar(None), false),
        Column::new("first_page_id".to_string(), DataType::UInt32, false),
        Column::new("last_page_id".to_string(), DataType::UInt32, false),
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

pub fn load_catalog_data(db: &mut Database) -> BustubxResult<()> {
    load_information_schema(&mut db.catalog)?;
    load_user_tables(db)?;
    Ok(())
}

fn load_user_tables(db: &mut Database) -> BustubxResult<()> {
    let table_tuples = db.run(&format!(
        "select * from {}.{}",
        INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_TABLES
    ))?;
    for table_tuple in table_tuples.into_iter() {
        let error = Err(BustubxError::Internal(format!(
            "Failed to decode table tuple: {:?}",
            table_tuple
        )));
        let ScalarValue::Varchar(Some(catalog)) = table_tuple.value(0)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_schema)) = table_tuple.value(1)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_name)) = table_tuple.value(2)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(first_page_id)) = table_tuple.value(3)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(last_page_id)) = table_tuple.value(4)? else {
            return error;
        };

        let column_tuples = db.run(&format!("select * from {}.{} where table_catalog = '{}' and table_schema = '{}' and table_name = '{}'",
                                            INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_COLUMNS, catalog, table_schema, table_name))?;
        let mut columns = vec![];
        for column_tuple in column_tuples.into_iter() {
            let error = Err(BustubxError::Internal(format!(
                "Failed to decode column tuple: {:?}",
                column_tuple
            )));
            let ScalarValue::Varchar(Some(column_name)) = column_tuple.value(3)? else {
                return error;
            };
            let ScalarValue::Varchar(Some(data_type_str)) = column_tuple.value(4)? else {
                return error;
            };
            let ScalarValue::Boolean(Some(nullable)) = column_tuple.value(5)? else {
                return error;
            };
            let data_type: DataType = data_type_str.as_str().try_into()?;
            columns.push(Column::new(column_name.clone(), data_type, *nullable));
        }
        let schema = Arc::new(Schema::new(columns));

        let table_heap = TableHeap {
            schema: schema.clone(),
            buffer_pool: db.buffer_pool.clone(),
            first_page_id: AtomicPageId::new(*first_page_id),
            last_page_id: AtomicPageId::new(*last_page_id),
        };
        let table_ref = TableReference::full(
            catalog.to_string(),
            table_schema.to_string(),
            table_name.to_string(),
        );
        db.catalog
            .tables
            .insert(table_ref.extend_to_full(), Arc::new(table_heap));
    }
    Ok(())
}

fn load_information_schema(catalog: &mut Catalog) -> BustubxResult<()> {
    let meta = catalog.buffer_pool.disk_manager.meta.read().unwrap();
    let information_schema_tables_first_page_id = meta.information_schema_tables_first_page_id;
    let information_schema_columns_first_page_id = meta.information_schema_columns_first_page_id;
    drop(meta);

    // load last page id
    let information_schema_tables_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_tables_first_page_id,
        TABLES_SCHMEA.clone(),
    )?;
    let information_schema_columns_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_columns_first_page_id,
        COLUMNS_SCHMEA.clone(),
    )?;

    let tables_table = TableHeap {
        schema: TABLES_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_tables_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_tables_last_page_id),
    };
    catalog
        .tables
        .insert(TABLES_TABLE_REF.extend_to_full(), Arc::new(tables_table));

    let columns_table = TableHeap {
        schema: COLUMNS_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_columns_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_columns_last_page_id),
    };
    catalog
        .tables
        .insert(COLUMNS_TABLE_REF.extend_to_full(), Arc::new(columns_table));
    Ok(())
}

fn load_table_last_page_id(
    catalog: &mut Catalog,
    first_page_id: PageId,
    schema: SchemaRef,
) -> BustubxResult<PageId> {
    let mut page_id = first_page_id;
    loop {
        let page = catalog.buffer_pool.fetch_page(page_id)?;
        let (table_page, _) = TablePageCodec::decode(&page.read().unwrap().data, schema.clone())?;
        catalog.buffer_pool.unpin_page(page_id, false)?;

        if table_page.header.next_page_id == INVALID_PAGE_ID {
            return Ok(page_id);
        } else {
            page_id = table_page.header.next_page_id;
        }
    }
}
