use crate::{
    buffer::buffer_pool::BufferPoolManager,
    catalog::schema::Schema,
    common::{config::INVALID_PAGE_ID, rid::Rid},
    storage::index_page::BPlusTreeLeafPage,
};

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

pub struct Context {
    pub root_page_id: PageId,
}

pub struct BPlusTreeIndex {
    pub index_name: String,
    pub buffer_pool_manager: BufferPoolManager,
    pub leaf_max_size: u32,
    pub internal_max_size: u32,
    pub root_page_id: PageId,
}
impl BPlusTreeIndex {
    pub fn new(
        index_name: String,
        buffer_pool_manager: BufferPoolManager,
        leaf_max_size: u32,
        internal_max_size: u32,
    ) -> Self {
        Self {
            index_name,
            buffer_pool_manager,
            leaf_max_size,
            internal_max_size,
            root_page_id: INVALID_PAGE_ID,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id == INVALID_PAGE_ID
    }

    pub fn get(&self, key: &Tuple) -> Vec<Rid> {
        unimplemented!()
    }

    pub fn insert(&mut self, key: &Tuple, rid: Rid) -> bool {
        if self.is_empty() {
            self.start_new_tree(key, rid);
            return true;
        }
        unimplemented!()
    }

    pub fn delete(&mut self, key: &Tuple, rid: Rid) {
        unimplemented!()
    }

    pub fn scan(&self, key: &Tuple) -> Vec<Rid> {
        unimplemented!()
    }

    fn start_new_tree(&mut self, key: &Tuple, rid: Rid) {
        let new_page = self
            .buffer_pool_manager
            .new_page()
            .expect("failed to start new tree");
        let new_page_id = new_page.page_id;

        let mut leaf_page = BPlusTreeLeafPage::new(self.leaf_max_size as u32);
        leaf_page.insert(key.clone(), rid);

        new_page.data = leaf_page.to_bytes();

        self.buffer_pool_manager.unpin_page(new_page_id, true);
        unimplemented!()
    }
}
