use crate::{buffer::buffer_pool::BufferPoolManager, catalog::schema::Schema, common::rid::Rid};

use super::{page::PageId, tuple::Tuple};

#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    // key schema与tuple schema的映射关系
    pub key_attrs: Vec<u32>,
    pub key_schema: Schema,
}
impl IndexMetadata {
    pub fn new(
        index_name: String,
        table_name: String,
        tuple_schema: &Schema,
        key_attrs: Vec<u32>,
    ) -> Self {
        let key_schema = Schema::copy_schema(tuple_schema, &key_attrs);
        Self {
            index_name,
            table_name,
            key_attrs,
            key_schema,
        }
    }
}

pub struct BPlusTreeIndex {
    pub index_name: String,
    pub buffer_pool_manager: BufferPoolManager,
    pub leaf_max_size: usize,
    pub internal_max_size: usize,
    pub root_page_id: PageId,
}
impl BPlusTreeIndex {
    pub fn new(
        index_name: String,
        buffer_pool_manager: BufferPoolManager,
        leaf_max_size: usize,
        internal_max_size: usize,
        root_page_id: PageId,
    ) -> Self {
        Self {
            index_name,
            buffer_pool_manager,
            leaf_max_size,
            internal_max_size,
            root_page_id,
        }
    }

    pub fn insert(&mut self, key: &Tuple, rid: Rid) -> bool {
        unimplemented!()
    }

    pub fn delete(&mut self, key: &Tuple, rid: Rid) {
        unimplemented!()
    }

    pub fn scan(&self, key: &Tuple) -> Vec<Rid> {
        unimplemented!()
    }
}
