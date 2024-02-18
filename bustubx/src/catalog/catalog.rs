use std::collections::HashMap;

use crate::buffer::TABLE_HEAP_BUFFER_POOL_SIZE;
use crate::catalog::SchemaRef;
use crate::common::TableReference;
use crate::{
    buffer::BufferPoolManager,
    storage::{
        index::{BPlusTreeIndex, IndexMetadata},
        TableHeap,
    },
    BustubxError, BustubxResult,
};

pub static DEFAULT_CATALOG_NAME: &str = "bustubx";
pub static DEFAULT_SCHEMA_NAME: &str = "public";

// table元信息
#[derive(Debug)]
pub struct TableInfo {
    pub schema: SchemaRef,
    pub name: String,
    pub table: TableHeap,
}

// index元信息
pub struct IndexInfo {
    pub key_schema: SchemaRef,
    pub name: String,
    pub index: BPlusTreeIndex,
    pub table_name: String,
}

pub struct Catalog {
    pub tables: HashMap<String, TableInfo>,
    pub indexes: HashMap<String, IndexInfo>,
    pub buffer_pool_manager: BufferPoolManager,
}

impl Catalog {
    pub fn new(buffer_pool_manager: BufferPoolManager) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            buffer_pool_manager,
        }
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> BustubxResult<&TableInfo> {
        if !self.tables.contains_key(table_ref.table()) {
            // 一个table对应一个buffer pool manager
            let buffer_pool_manager = BufferPoolManager::new(
                TABLE_HEAP_BUFFER_POOL_SIZE,
                self.buffer_pool_manager.disk_manager.clone(),
            );
            let table_heap = TableHeap::try_new(schema.clone(), buffer_pool_manager)?;
            let table_info = TableInfo {
                schema,
                name: table_ref.table().to_string(),
                table: table_heap,
            };

            self.tables
                .insert(table_ref.table().to_string(), table_info);
        }

        self.tables
            .get(table_ref.table())
            .ok_or(BustubxError::Internal("Failed to create table".to_string()))
    }

    pub fn table(&self, table_ref: &TableReference) -> BustubxResult<&TableInfo> {
        self.tables
            .get(table_ref.table())
            .ok_or(BustubxError::Internal(format!(
                "Not found the table {}",
                table_ref
            )))
    }

    pub fn table_mut(&mut self, table_ref: &TableReference) -> BustubxResult<&mut TableInfo> {
        self.tables
            .get_mut(table_ref.table())
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
        let table_info = self.table(table_ref)?;
        let tuple_schema = table_info.schema.clone();
        let key_schema = tuple_schema.project(&key_attrs)?;

        let index_metadata = IndexMetadata::new(
            index_name.clone(),
            table_ref.table().to_string(),
            tuple_schema.clone(),
            key_attrs,
        );
        // one buffer pool manager for one index
        let buffer_pool_manager = BufferPoolManager::new(
            TABLE_HEAP_BUFFER_POOL_SIZE,
            self.buffer_pool_manager.disk_manager.clone(),
        );
        // TODO compute leaf_max_size and internal_max_size
        let b_plus_tree_index = BPlusTreeIndex::new(index_metadata, buffer_pool_manager, 10, 10);

        let index_info = IndexInfo {
            key_schema,
            name: index_name.clone(),
            index: b_plus_tree_index,
            table_name: table_ref.table().to_string(),
        };
        self.indexes.insert(index_name.clone(), index_info);
        self.indexes
            .get(&index_name)
            .ok_or(BustubxError::Internal("Failed to create table".to_string()))
    }

    pub fn get_index_by_name(&self, index_name: &str) -> Option<&IndexInfo> {
        self.indexes.get(index_name)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, sync::Arc};

    use crate::common::TableReference;
    use crate::{
        buffer::BufferPoolManager,
        catalog::{Column, DataType, Schema},
        storage::DiskManager,
    };

    #[test]
    pub fn test_catalog_create_table() {
        let db_path = "./test_catalog_create_table.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let table_ref1 = TableReference::bare("test_table1".to_string());
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, true),
            Column::new("b".to_string(), DataType::Int16, true),
            Column::new("c".to_string(), DataType::Int32, true),
        ]));
        let table_info = catalog
            .create_table(table_ref1.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.name, table_ref1.table());
        assert_eq!(table_info.schema, schema);

        let table_ref2 = TableReference::bare("test_table2".to_string());
        let schema = Arc::new(Schema::new(vec![
            Column::new("d".to_string(), DataType::Int32, true),
            Column::new("e".to_string(), DataType::Int16, true),
            Column::new("f".to_string(), DataType::Int8, true),
        ]));
        let table_info = catalog
            .create_table(table_ref2.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.name, table_ref2.table());
        assert_eq!(table_info.schema, schema);

        let table_info = catalog.table(&table_ref1).unwrap();
        assert_eq!(table_info.name, table_ref1.table());
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.table(&table_ref2).unwrap();
        assert_eq!(table_info.name, table_ref2.table());
        assert_eq!(table_info.schema.column_count(), 3);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_catalog_create_index() {
        let db_path = "./test_catalog_create_index.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let table_ref = TableReference::bare("test_table1".to_string());
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, true),
            Column::new("b".to_string(), DataType::Int16, true),
            Column::new("c".to_string(), DataType::Int32, true),
        ]));
        let _ = catalog.create_table(table_ref.clone(), schema);

        let index_name1 = "test_index1".to_string();
        let key_attrs = vec![0, 2];
        let index_info = catalog
            .create_index(index_name1.clone(), &table_ref, key_attrs)
            .unwrap();
        assert_eq!(index_info.name, index_name1);
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
        assert_eq!(index_info.name, index_name2);
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

        let index_info = catalog.get_index_by_name(index_name1.as_str());
        assert!(index_info.is_some());
        let index_info = index_info.unwrap();
        assert_eq!(index_info.name, index_name1);

        let _ = remove_file(db_path);
    }
}
