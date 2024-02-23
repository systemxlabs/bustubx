use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::catalog::{
    SchemaRef, COLUMNS_SCHMEA, COLUMNS_TABLE_REF, TABLES_SCHMEA, TABLES_TABLE_REF,
};
use crate::common::{FullTableRef, TableReference};
use crate::storage::{TupleMeta, BPLUS_INTERNAL_PAGE_MAX_SIZE, BPLUS_LEAF_PAGE_MAX_SIZE};
use crate::{
    buffer::BufferPoolManager,
    storage::{
        index::{BPlusTreeIndex, IndexMetadata},
        TableHeap,
    },
    BustubxError, BustubxResult, Tuple,
};

pub static DEFAULT_CATALOG_NAME: &str = "bustubx";
pub static DEFAULT_SCHEMA_NAME: &str = "public";

/// catalog, schema, table, index
pub type FullIndexRef = (String, String, String, String);

// index元信息
pub struct IndexInfo {
    pub key_schema: SchemaRef,
    pub index: BPlusTreeIndex,
    pub table_name: String,
}

pub struct Catalog {
    pub tables: HashMap<FullTableRef, Arc<TableHeap>>,
    pub indexes: HashMap<FullIndexRef, IndexInfo>,
    pub buffer_pool: Arc<BufferPoolManager>,
}

impl Catalog {
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            buffer_pool,
        }
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> BustubxResult<Arc<TableHeap>> {
        // TODO fail if database not created
        let full_table_ref = table_ref.extend_to_full();
        if !self.tables.contains_key(&full_table_ref) {
            let table_heap = TableHeap::try_new(schema.clone(), self.buffer_pool.clone())?;
            let first_page_id = table_heap.first_page_id.load(Ordering::SeqCst);
            let last_page_id = table_heap.last_page_id.load(Ordering::SeqCst);

            self.tables
                .insert(full_table_ref.clone(), Arc::new(table_heap));

            // update system table
            let tables_table = self
                .tables
                .get_mut(&TABLES_TABLE_REF.extend_to_full())
                .ok_or(BustubxError::Internal(
                    "Cannot find tables table".to_string(),
                ))?;
            let tuple_meta = TupleMeta {
                insert_txn_id: 0,
                delete_txn_id: 0,
                is_deleted: false,
            };
            let tuple = Tuple::new(
                TABLES_SCHMEA.clone(),
                vec![
                    full_table_ref.0.clone().into(),
                    full_table_ref.1.clone().into(),
                    full_table_ref.2.clone().into(),
                    first_page_id.into(),
                    last_page_id.into(),
                ],
            );
            tables_table.insert_tuple(&tuple_meta, &tuple)?;

            let columns_table = self
                .tables
                .get_mut(&COLUMNS_TABLE_REF.extend_to_full())
                .ok_or(BustubxError::Internal(
                    "Cannot find columns table".to_string(),
                ))?;
            for col in schema.columns.iter() {
                let sql_type: sqlparser::ast::DataType = (&col.data_type).into();
                let tuple = Tuple::new(
                    COLUMNS_SCHMEA.clone(),
                    vec![
                        full_table_ref.0.clone().into(),
                        full_table_ref.1.clone().into(),
                        full_table_ref.2.clone().into(),
                        col.name.clone().into(),
                        format!("{sql_type}").into(),
                        col.nullable.into(),
                    ],
                );
                columns_table.insert_tuple(&tuple_meta, &tuple)?;
            }
        }

        self.tables
            .get(&full_table_ref)
            .cloned()
            .ok_or(BustubxError::Internal("Failed to create table".to_string()))
    }

    pub fn table_heap(&self, table_ref: &TableReference) -> BustubxResult<Arc<TableHeap>> {
        self.tables
            .get(&table_ref.extend_to_full())
            .cloned()
            .ok_or(BustubxError::Internal(format!(
                "Not found the table {}",
                table_ref
            )))
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_ref: &TableReference,
        key_attrs: Vec<usize>,
    ) -> BustubxResult<&IndexInfo> {
        let (catalog, schema, table) = table_ref.extend_to_full();
        let full_index_ref = (catalog, schema, table, index_name.clone());

        let table_info = self.table_heap(table_ref)?;
        let tuple_schema = table_info.schema.clone();
        let key_schema = tuple_schema.project(&key_attrs)?;

        let index_metadata = IndexMetadata::new(
            index_name.clone(),
            table_ref.table().to_string(),
            tuple_schema.clone(),
            key_attrs,
        );
        let b_plus_tree_index = BPlusTreeIndex::new(
            index_metadata,
            self.buffer_pool.clone(),
            BPLUS_LEAF_PAGE_MAX_SIZE as u32,
            BPLUS_INTERNAL_PAGE_MAX_SIZE as u32,
        );

        let index_info = IndexInfo {
            key_schema,
            index: b_plus_tree_index,
            table_name: table_ref.table().to_string(),
        };
        self.indexes.insert(full_index_ref.clone(), index_info);
        self.indexes
            .get(&full_index_ref)
            .ok_or(BustubxError::Internal("Failed to create table".to_string()))
    }

    pub fn get_index_by_name(
        &self,
        table_ref: &TableReference,
        index_name: &str,
    ) -> Option<&IndexInfo> {
        let (catalog, schema, table) = table_ref.extend_to_full();
        let full_index_ref = (catalog, schema, table, index_name.to_string());
        self.indexes.get(&full_index_ref)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::common::TableReference;
    use crate::{
        buffer::BufferPoolManager,
        catalog::{Column, DataType, Schema},
        storage::DiskManager,
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
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, Arc::new(disk_manager)));
        let mut catalog = super::Catalog::new(buffer_pool);

        let table_ref = TableReference::bare("test_table1");
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int16, true),
            Column::new("c", DataType::Int32, true),
        ]));
        let _ = catalog.create_table(table_ref.clone(), schema);

        let index_name1 = "test_index1".to_string();
        let key_attrs = vec![0, 2];
        let index_info = catalog
            .create_index(index_name1.clone(), &table_ref, key_attrs)
            .unwrap();
        assert_eq!(index_info.table_name, table_ref.table());
        assert_eq!(index_info.key_schema.column_count(), 2);
        assert_eq!(
            index_info.key_schema.column_with_index(0).unwrap().name,
            "a".to_string()
        );
        assert_eq!(
            index_info
                .key_schema
                .column_with_index(0)
                .unwrap()
                .data_type,
            DataType::Int8
        );
        assert_eq!(
            index_info.key_schema.column_with_index(1).unwrap().name,
            "c".to_string()
        );
        assert_eq!(
            index_info
                .key_schema
                .column_with_index(1)
                .unwrap()
                .data_type,
            DataType::Int32
        );

        let index_name2 = "test_index2".to_string();
        let key_attrs = vec![1];
        let index_info = catalog
            .create_index(index_name2.clone(), &table_ref, key_attrs)
            .unwrap();
        assert_eq!(index_info.table_name, table_ref.table());
        assert_eq!(index_info.key_schema.column_count(), 1);
        assert_eq!(
            index_info.key_schema.column_with_index(0).unwrap().name,
            "b".to_string()
        );
        assert_eq!(
            index_info
                .key_schema
                .column_with_index(0)
                .unwrap()
                .data_type,
            DataType::Int16
        );

        let index_info = catalog.get_index_by_name(&table_ref, index_name1.as_str());
        assert!(index_info.is_some());
        let _index_info = index_info.unwrap();
    }
}
