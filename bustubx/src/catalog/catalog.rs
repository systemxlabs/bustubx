use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::catalog::{
    SchemaRef, COLUMNS_SCHMEA, INFORMATION_SCHEMA_COLUMNS, INFORMATION_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMAS, INFORMATION_SCHEMA_TABLES, SCHEMAS_SCHMEA, TABLES_SCHMEA,
};
use crate::common::TableReference;
use crate::storage::{BPLUS_INTERNAL_PAGE_MAX_SIZE, BPLUS_LEAF_PAGE_MAX_SIZE, EMPTY_TUPLE_META};
use crate::{
    buffer::BufferPoolManager,
    storage::{index::BPlusTreeIndex, TableHeap},
    BustubxError, BustubxResult, Tuple,
};

pub static DEFAULT_CATALOG_NAME: &str = "bustubx";
pub static DEFAULT_SCHEMA_NAME: &str = "public";

#[derive(Debug)]
pub struct Catalog {
    pub schemas: HashMap<String, CatalogSchema>,
    pub buffer_pool: Arc<BufferPoolManager>,
}

#[derive(Debug)]
pub struct CatalogSchema {
    pub name: String,
    pub tables: HashMap<String, CatalogTable>,
}

impl CatalogSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct CatalogTable {
    pub name: String,
    pub table: Arc<TableHeap>,
    pub indexes: HashMap<String, Arc<BPlusTreeIndex>>,
}

impl CatalogTable {
    pub fn new(name: impl Into<String>, table: Arc<TableHeap>) -> Self {
        Self {
            name: name.into(),
            table,
            indexes: HashMap::new(),
        }
    }
}

impl Catalog {
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Self {
        Self {
            schemas: HashMap::new(),
            buffer_pool,
        }
    }

    pub fn create_schema(&mut self, schema_name: impl Into<String>) -> BustubxResult<()> {
        let schema_name = schema_name.into();
        if self.schemas.contains_key(&schema_name) {
            return Err(BustubxError::Storage(
                "Cannot create duplicated schema".to_string(),
            ));
        }
        self.schemas
            .insert(schema_name.clone(), CatalogSchema::new(schema_name.clone()));

        // update system table
        let Some(information_schema) = self.schemas.get_mut(INFORMATION_SCHEMA_NAME) else {
            return Err(BustubxError::Internal(
                "catalog schema information_schema not created yet".to_string(),
            ));
        };
        let Some(schemas_table) = information_schema
            .tables
            .get_mut(INFORMATION_SCHEMA_SCHEMAS)
        else {
            return Err(BustubxError::Internal(
                "table information_schema.schemas not created yet".to_string(),
            ));
        };

        let tuple = Tuple::new(
            SCHEMAS_SCHMEA.clone(),
            vec![
                DEFAULT_CATALOG_NAME.to_string().into(),
                schema_name.clone().into(),
            ],
        );
        schemas_table
            .table
            .insert_tuple(&EMPTY_TUPLE_META, &tuple)?;
        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> BustubxResult<Arc<TableHeap>> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get_mut(&catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        if catalog_schema.tables.contains_key(table_ref.table()) {
            return Err(BustubxError::Storage(
                "Cannot create duplicated table".to_string(),
            ));
        }
        let table_heap = Arc::new(TableHeap::try_new(
            schema.clone(),
            self.buffer_pool.clone(),
        )?);
        let catalog_table = CatalogTable {
            name: table_name.clone(),
            table: table_heap.clone(),
            indexes: HashMap::new(),
        };
        catalog_schema
            .tables
            .insert(table_name.clone(), catalog_table);

        // update system table
        let Some(information_schema) = self.schemas.get_mut(INFORMATION_SCHEMA_NAME) else {
            return Err(BustubxError::Internal(
                "catalog schema information_schema not created yet".to_string(),
            ));
        };
        let Some(tables_table) = information_schema.tables.get_mut(INFORMATION_SCHEMA_TABLES)
        else {
            return Err(BustubxError::Internal(
                "table information_schema.tables not created yet".to_string(),
            ));
        };

        let tuple = Tuple::new(
            TABLES_SCHMEA.clone(),
            vec![
                catalog_name.clone().into(),
                catalog_schema_name.clone().into(),
                table_name.clone().into(),
                (table_heap.first_page_id.load(Ordering::SeqCst)).into(),
            ],
        );
        tables_table.table.insert_tuple(&EMPTY_TUPLE_META, &tuple)?;

        let Some(columns_table) = information_schema
            .tables
            .get_mut(INFORMATION_SCHEMA_COLUMNS)
        else {
            return Err(BustubxError::Internal(
                "table information_schema.columns not created yet".to_string(),
            ));
        };
        for col in schema.columns.iter() {
            let sql_type: sqlparser::ast::DataType = (&col.data_type).into();
            let tuple = Tuple::new(
                COLUMNS_SCHMEA.clone(),
                vec![
                    catalog_name.clone().into(),
                    catalog_schema_name.clone().into(),
                    table_name.clone().into(),
                    col.name.clone().into(),
                    format!("{sql_type}").into(),
                    col.nullable.into(),
                ],
            );
            columns_table
                .table
                .insert_tuple(&EMPTY_TUPLE_META, &tuple)?;
        }

        Ok(table_heap)
    }

    pub fn table_heap(&self, table_ref: &TableReference) -> BustubxResult<Arc<TableHeap>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(BustubxError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table.table.clone())
    }

    pub fn table_indexes(
        &self,
        table_ref: &TableReference,
    ) -> BustubxResult<Vec<Arc<BPlusTreeIndex>>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(BustubxError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table.indexes.values().cloned().collect())
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_ref: &TableReference,
        key_schema: SchemaRef,
    ) -> BustubxResult<Arc<BPlusTreeIndex>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get_mut(&catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get_mut(&table_name) else {
            return Err(BustubxError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        if catalog_table.indexes.contains_key(&index_name) {
            return Err(BustubxError::Storage(
                "Cannot create duplicated index".to_string(),
            ));
        }

        let b_plus_tree_index = Arc::new(BPlusTreeIndex::new(
            key_schema.clone(),
            self.buffer_pool.clone(),
            BPLUS_LEAF_PAGE_MAX_SIZE as u32,
            BPLUS_INTERNAL_PAGE_MAX_SIZE as u32,
        ));
        catalog_table
            .indexes
            .insert(index_name, b_plus_tree_index.clone());

        Ok(b_plus_tree_index)
    }

    pub fn index(
        &self,
        table_ref: &TableReference,
        index_name: &str,
    ) -> BustubxResult<Option<Arc<BPlusTreeIndex>>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(BustubxError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table.indexes.get(index_name).cloned())
    }

    pub fn load_schema(&mut self, name: impl Into<String>, schema: CatalogSchema) {
        self.schemas.insert(name.into(), schema);
    }

    pub fn load_table(
        &mut self,
        table_ref: TableReference,
        table: CatalogTable,
    ) -> BustubxResult<()> {
        let catalog_schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        let table_name = table_ref.table().to_string();
        let Some(catalog_schema) = self.schemas.get_mut(catalog_schema_name) else {
            return Err(BustubxError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        catalog_schema.tables.insert(table_name, table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::common::TableReference;
    use crate::{
        catalog::{Column, DataType, Schema},
        Database,
    };

    #[test]
    pub fn test_catalog_create_table() {
        let mut db = Database::new_temp().unwrap();

        let table_ref1 = TableReference::bare("test_table1");
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int16, true),
            Column::new("c", DataType::Int32, true),
        ]));
        let table_info = db
            .catalog
            .create_table(table_ref1.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.schema, schema);

        let table_ref2 = TableReference::bare("test_table2");
        let schema = Arc::new(Schema::new(vec![
            Column::new("d", DataType::Int32, true),
            Column::new("e", DataType::Int16, true),
            Column::new("f", DataType::Int8, true),
        ]));
        let table_info = db
            .catalog
            .create_table(table_ref2.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.schema, schema);

        let table_info = db.catalog.table_heap(&table_ref1).unwrap();
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = db.catalog.table_heap(&table_ref2).unwrap();
        assert_eq!(table_info.schema.column_count(), 3);
    }

    #[test]
    pub fn test_catalog_create_index() {
        let mut db = Database::new_temp().unwrap();

        let table_ref = TableReference::bare("test_table1");
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int16, true),
            Column::new("c", DataType::Int32, true),
        ]));
        let _ = db.catalog.create_table(table_ref.clone(), schema.clone());

        let index_name1 = "test_index1".to_string();
        let key_schema1 = schema.project(&[0, 2]).unwrap();
        let index1 = db
            .catalog
            .create_index(index_name1.clone(), &table_ref, key_schema1.clone())
            .unwrap();
        assert_eq!(index1.key_schema, key_schema1);

        let index_name2 = "test_index2".to_string();
        let key_schema2 = schema.project(&[1]).unwrap();
        let index2 = db
            .catalog
            .create_index(index_name2.clone(), &table_ref, key_schema2.clone())
            .unwrap();
        assert_eq!(index2.key_schema, key_schema2);

        let index3 = db
            .catalog
            .index(&table_ref, index_name1.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(index3.key_schema, key_schema1);
    }
}
