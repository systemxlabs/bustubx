use std::{collections::HashMap, sync::atomic::AtomicU32};

use crate::{
    buffer::buffer_pool::BufferPoolManager,
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    storage::{
        index::{BPlusTreeIndex, IndexMetadata},
        table_heap::TableHeap,
    },
};

use super::schema::Schema;

pub type TableOid = u32;
pub type IndexOid = u32;

pub static DEFAULT_CATALOG_NAME: &str = "bustubx";
pub static DEFAULT_SCHEMA_NAME: &str = "bustubx";

// table元信息
#[derive(Debug)]
pub struct TableInfo {
    pub schema: Schema,
    pub name: String,
    pub table: TableHeap,
    pub oid: TableOid,
}

// index元信息
pub struct IndexInfo {
    pub key_schema: Schema,
    pub name: String,
    pub index: BPlusTreeIndex,
    pub table_name: String,
    pub oid: IndexOid,
}

pub struct Catalog {
    pub tables: HashMap<TableOid, TableInfo>,
    pub table_names: HashMap<String, TableOid>,
    pub next_table_oid: AtomicU32,
    pub indexes: HashMap<IndexOid, IndexInfo>,
    // table_name -> index_name -> index_oid
    pub index_names: HashMap<String, HashMap<String, IndexOid>>,
    pub next_index_oid: AtomicU32,
    pub buffer_pool_manager: BufferPoolManager,
}
impl Catalog {
    pub fn new(buffer_pool_manager: BufferPoolManager) -> Self {
        Self {
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_oid: AtomicU32::new(0),
            indexes: HashMap::new(),
            index_names: HashMap::new(),
            next_index_oid: AtomicU32::new(0),
            buffer_pool_manager: buffer_pool_manager,
        }
    }

    pub fn create_table(&mut self, table_name: String, schema: Schema) -> Option<&TableInfo> {
        if self.table_names.contains_key(&table_name) {
            return None;
        }

        // 一个table对应一个buffer pool manager
        let buffer_pool_manager = BufferPoolManager::new(
            TABLE_HEAP_BUFFER_POOL_SIZE,
            self.buffer_pool_manager.disk_manager.clone(),
        );
        let table_heap = TableHeap::new(buffer_pool_manager);
        let table_oid = self
            .next_table_oid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let table_info = TableInfo {
            schema,
            name: table_name.clone(),
            table: table_heap,
            oid: table_oid,
        };

        self.tables.insert(table_oid, table_info);
        self.table_names.insert(table_name.clone(), table_oid);
        self.index_names.insert(table_name, HashMap::new());
        self.tables.get(&table_oid)
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableInfo> {
        self.table_names
            .get(table_name)
            .and_then(|oid| self.tables.get(oid))
    }
    pub fn get_mut_table_by_name(&mut self, table_name: &str) -> Option<&mut TableInfo> {
        self.table_names
            .get(table_name)
            .and_then(|oid| self.tables.get_mut(oid))
    }

    pub fn get_table_by_oid(&self, oid: TableOid) -> Option<&TableInfo> {
        self.tables.get(&oid)
    }

    pub fn get_mut_table_by_oid(&mut self, table_oid: TableOid) -> Option<&mut TableInfo> {
        self.tables.get_mut(&table_oid)
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        key_attrs: Vec<u32>,
    ) -> &IndexInfo {
        let table_info = self
            .get_table_by_name(&table_name)
            .expect("table not found");
        let tuple_schema = table_info.schema.clone();
        let key_schema = Schema::copy_schema(&tuple_schema, &key_attrs);

        let index_metadata = IndexMetadata::new(
            index_name.clone(),
            table_name.clone(),
            &tuple_schema,
            key_attrs,
        );
        // one buffer pool manager for one index
        let buffer_pool_manager = BufferPoolManager::new(
            TABLE_HEAP_BUFFER_POOL_SIZE,
            self.buffer_pool_manager.disk_manager.clone(),
        );
        // TODO compute leaf_max_size and internal_max_size
        let b_plus_tree_index = BPlusTreeIndex::new(index_metadata, buffer_pool_manager, 10, 10);

        let index_oid = self
            .next_index_oid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let index_info = IndexInfo {
            key_schema,
            name: index_name.clone(),
            index: b_plus_tree_index,
            table_name: table_name.clone(),
            oid: index_oid,
        };
        self.indexes.insert(index_oid, index_info);
        if self.index_names.contains_key(&table_name) {
            self.index_names
                .get_mut(&table_name)
                .unwrap()
                .insert(index_name, index_oid);
        } else {
            let mut index_names = HashMap::new();
            index_names.insert(index_name, index_oid);
            self.index_names.insert(table_name, index_names);
        }
        self.indexes.get(&index_oid).unwrap()
    }

    pub fn get_index_by_oid(&self, oid: IndexOid) -> Option<&IndexInfo> {
        self.indexes.get(&oid)
    }

    pub fn get_index_by_name(&self, table_name: &str, index_name: &str) -> Option<&IndexInfo> {
        self.index_names
            .get(table_name)
            .and_then(|index_names| index_names.get(index_name))
            .and_then(|index_oid| self.indexes.get(index_oid))
    }

    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&IndexInfo> {
        self.index_names
            .get(table_name)
            .map(|index_names| {
                index_names
                    .iter()
                    .map(|(_, index_oid)| self.indexes.get(index_oid).unwrap())
                    .collect()
            })
            .unwrap_or(vec![])
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, sync::Arc};

    use crate::{
        buffer::buffer_pool::BufferPoolManager,
        catalog::{column::Column, data_type::DataType, schema::Schema},
        storage::disk_manager,
    };

    #[test]
    pub fn test_catalog_create_table() {
        let db_path = "./test_catalog_create_table.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let table_name = "test_table1".to_string();
        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8),
            Column::new("b".to_string(), DataType::Int16),
            Column::new("c".to_string(), DataType::Int32),
        ]);
        let table_info = catalog.create_table(table_name.clone(), schema);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name);
        assert_eq!(table_info.schema.column_count(), 3);
        assert_eq!(
            table_info.schema.get_col_by_index(0).unwrap().name,
            "a".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(0).unwrap().data_type,
            DataType::Int8
        );
        assert_eq!(
            table_info.schema.get_col_by_index(1).unwrap().name,
            "b".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(1).unwrap().data_type,
            DataType::Int16
        );
        assert_eq!(
            table_info.schema.get_col_by_index(2).unwrap().name,
            "c".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(2).unwrap().data_type,
            DataType::Int32
        );
        assert_eq!(table_info.oid, 0);

        let table_name = "test_table2".to_string();
        let schema = Schema::new(vec![
            Column::new("d".to_string(), DataType::Int32),
            Column::new("e".to_string(), DataType::Int16),
            Column::new("f".to_string(), DataType::Int8),
        ]);
        let table_info = catalog.create_table(table_name.clone(), schema);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name);
        assert_eq!(table_info.schema.column_count(), 3);
        assert_eq!(
            table_info.schema.get_col_by_index(0).unwrap().name,
            "d".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(0).unwrap().data_type,
            DataType::Int32
        );
        assert_eq!(
            table_info.schema.get_col_by_index(1).unwrap().name,
            "e".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(1).unwrap().data_type,
            DataType::Int16
        );
        assert_eq!(
            table_info.schema.get_col_by_index(2).unwrap().name,
            "f".to_string()
        );
        assert_eq!(
            table_info.schema.get_col_by_index(2).unwrap().data_type,
            DataType::Int8
        );
        assert_eq!(table_info.oid, 1);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_catalog_get_table() {
        let db_path = "./test_catalog_get_table.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let table_name1 = "test_table1".to_string();
        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8),
            Column::new("b".to_string(), DataType::Int16),
            Column::new("c".to_string(), DataType::Int32),
        ]);
        let _ = catalog.create_table(table_name1.clone(), schema);

        let table_name2 = "test_table2".to_string();
        let schema = Schema::new(vec![
            Column::new("d".to_string(), DataType::Int32),
            Column::new("e".to_string(), DataType::Int16),
            Column::new("f".to_string(), DataType::Int8),
        ]);
        let _ = catalog.create_table(table_name2.clone(), schema);

        let table_info = catalog.get_table_by_name(&table_name1);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name1);
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_name(&table_name2);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name2);
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_name("test_table3");
        assert!(table_info.is_none());

        let table_info = catalog.get_table_by_oid(0);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name1);
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_oid(1);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, table_name2);
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_oid(2);
        assert!(table_info.is_none());

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_catalog_create_index() {
        let db_path = "./test_catalog_create_index.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let table_name = "test_table1".to_string();
        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8),
            Column::new("b".to_string(), DataType::Int16),
            Column::new("c".to_string(), DataType::Int32),
        ]);
        let _ = catalog.create_table(table_name.clone(), schema);

        let index_name1 = "test_index1".to_string();
        let key_attrs = vec![0, 2];
        let index_info = catalog.create_index(index_name1.clone(), table_name.clone(), key_attrs);
        assert_eq!(index_info.name, index_name1);
        assert_eq!(index_info.table_name, table_name);
        assert_eq!(index_info.key_schema.column_count(), 2);
        assert_eq!(
            index_info.key_schema.get_col_by_index(0).unwrap().name,
            "a".to_string()
        );
        assert_eq!(
            index_info.key_schema.get_col_by_index(0).unwrap().data_type,
            DataType::Int8
        );
        assert_eq!(
            index_info.key_schema.get_col_by_index(1).unwrap().name,
            "c".to_string()
        );
        assert_eq!(
            index_info.key_schema.get_col_by_index(1).unwrap().data_type,
            DataType::Int32
        );
        assert_eq!(index_info.oid, 0);

        let index_name2 = "test_index2".to_string();
        let key_attrs = vec![1];
        let index_info = catalog.create_index(index_name2.clone(), table_name.clone(), key_attrs);
        assert_eq!(index_info.name, index_name2);
        assert_eq!(index_info.table_name, table_name);
        assert_eq!(index_info.key_schema.column_count(), 1);
        assert_eq!(
            index_info.key_schema.get_col_by_index(0).unwrap().name,
            "b".to_string()
        );
        assert_eq!(
            index_info.key_schema.get_col_by_index(0).unwrap().data_type,
            DataType::Int16
        );
        assert_eq!(index_info.oid, 1);

        let index_info = catalog.get_index_by_name(table_name.as_str(), index_name1.as_str());
        assert!(index_info.is_some());
        let index_info = index_info.unwrap();
        assert_eq!(index_info.name, index_name1);

        let index_info = catalog.get_index_by_oid(1);
        assert!(index_info.is_some());
        let index_info = index_info.unwrap();
        assert_eq!(index_info.name, index_name2);

        let table_indexes = catalog.get_table_indexes(table_name.as_str());
        assert_eq!(table_indexes.len(), 2);
        assert!(table_indexes[0].name == index_name1 || table_indexes[0].name == index_name2);
        assert!(table_indexes[1].name == index_name1 || table_indexes[1].name == index_name2);

        let _ = remove_file(db_path);
    }
}
