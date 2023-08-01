use std::{collections::HashMap, sync::atomic::AtomicU32};

use crate::{
    buffer::{self, buffer_pool::BufferPoolManager},
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    storage::{index::BPlusTreeIndex, table_heap::TableHeap},
};

use super::schema::Schema;

pub type TableOid = u32;
pub type IndexOid = u32;

// table元信息
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

    pub fn get_table_by_oid(&self, oid: TableOid) -> Option<&TableInfo> {
        self.tables.get(&oid)
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        key_attrs: &[u32],
    ) -> &IndexInfo {
        unimplemented!()
    }

    pub fn get_index_by_oid(&self, oid: IndexOid) -> Option<&IndexInfo> {
        unimplemented!()
    }

    pub fn get_index_by_name(&self, table_name: &str, index_name: &str) -> Option<&IndexInfo> {
        unimplemented!()
    }

    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&IndexInfo> {
        unimplemented!()
    }
}

mod tests {
    use std::{fs::remove_file, sync::Arc};

    use crate::{
        buffer::buffer_pool::BufferPoolManager,
        catalog::{
            column::{Column, DataType},
            schema::Schema,
        },
        storage::disk_manager,
    };

    #[test]
    pub fn test_catalog_create_table() {
        let db_path = "./test_catalog_create_table.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
            Column::new("c".to_string(), DataType::Integer, 0),
        ]);
        let table_info = catalog.create_table("test_table1".to_string(), schema);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table1");
        assert_eq!(table_info.schema.column_count(), 3);
        assert_eq!(table_info.schema.get_by_index(0).unwrap().column_name, "a");
        assert_eq!(
            table_info.schema.get_by_index(0).unwrap().column_type,
            DataType::TinyInt
        );
        assert_eq!(table_info.schema.get_by_index(1).unwrap().column_name, "b");
        assert_eq!(
            table_info.schema.get_by_index(1).unwrap().column_type,
            DataType::SmallInt
        );
        assert_eq!(table_info.schema.get_by_index(2).unwrap().column_name, "c");
        assert_eq!(
            table_info.schema.get_by_index(2).unwrap().column_type,
            DataType::Integer
        );
        assert_eq!(table_info.oid, 0);

        let schema = Schema::new(vec![
            Column::new("d".to_string(), DataType::Integer, 0),
            Column::new("e".to_string(), DataType::SmallInt, 0),
            Column::new("f".to_string(), DataType::TinyInt, 0),
        ]);
        let table_info = catalog.create_table("test_table2".to_string(), schema);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table2");
        assert_eq!(table_info.schema.column_count(), 3);
        assert_eq!(table_info.schema.get_by_index(0).unwrap().column_name, "d");
        assert_eq!(
            table_info.schema.get_by_index(0).unwrap().column_type,
            DataType::Integer
        );
        assert_eq!(table_info.schema.get_by_index(1).unwrap().column_name, "e");
        assert_eq!(
            table_info.schema.get_by_index(1).unwrap().column_type,
            DataType::SmallInt
        );
        assert_eq!(table_info.schema.get_by_index(2).unwrap().column_name, "f");
        assert_eq!(
            table_info.schema.get_by_index(2).unwrap().column_type,
            DataType::TinyInt
        );
        assert_eq!(table_info.oid, 1);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_catalog_get_table() {
        let db_path = "./test_catalog_get_table.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut catalog = super::Catalog::new(buffer_pool_manager);

        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
            Column::new("c".to_string(), DataType::Integer, 0),
        ]);
        let _ = catalog.create_table("test_table1".to_string(), schema);

        let schema = Schema::new(vec![
            Column::new("d".to_string(), DataType::Integer, 0),
            Column::new("e".to_string(), DataType::SmallInt, 0),
            Column::new("f".to_string(), DataType::TinyInt, 0),
        ]);
        let _ = catalog.create_table("test_table2".to_string(), schema);

        let table_info = catalog.get_table_by_name("test_table1");
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table1");
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_name("test_table2");
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table2");
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_name("test_table3");
        assert!(table_info.is_none());

        let table_info = catalog.get_table_by_oid(0);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table1");
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_oid(1);
        assert!(table_info.is_some());
        let table_info = table_info.unwrap();
        assert_eq!(table_info.name, "test_table2");
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = catalog.get_table_by_oid(2);
        assert!(table_info.is_none());

        let _ = remove_file(db_path);
    }
}
