use std::{
    collections::HashMap,
    sync::{atomic::{AtomicPtr, AtomicU32}, Arc, Mutex},
};

use crate::{
    buffer::buffer_pool::BufferPoolManager,
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
        // let table_heap = TableHeap::new(self.buffer_pool_manager);

        unimplemented!()
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableInfo> {
        unimplemented!()
    }

    pub fn get_table_by_oid(&self, oid: TableOid) -> Option<&TableInfo> {
        unimplemented!()
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
