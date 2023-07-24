use crate::{
    buffer::buffer_pool::BufferPoolManager,
    catalog::schema::Schema,
    common::{config::INVALID_PAGE_ID, rid::Rid},
    storage::index_page::{BPlusTreeLeafPage, BPlusTreePage},
};

use super::{page::PageId, tuple::Tuple};

// 索引元信息
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

// B+树索引
pub struct BPlusTreeIndex {
    pub index_metadata: IndexMetadata,
    pub buffer_pool_manager: BufferPoolManager,
    pub leaf_max_size: u32,
    pub internal_max_size: u32,
    pub root_page_id: PageId,
}
impl BPlusTreeIndex {
    pub fn new(
        index_metadata: IndexMetadata,
        buffer_pool_manager: BufferPoolManager,
        leaf_max_size: u32,
        internal_max_size: u32,
    ) -> Self {
        Self {
            index_metadata,
            buffer_pool_manager,
            leaf_max_size,
            internal_max_size,
            root_page_id: INVALID_PAGE_ID,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id == INVALID_PAGE_ID
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
        leaf_page.insert(key.clone(), rid, &self.index_metadata.key_schema);

        new_page.data = leaf_page.to_bytes();

        self.buffer_pool_manager.unpin_page(new_page_id, true);
    }

    // 找到叶子节点上对应的Value
    fn get(&mut self, key: &Tuple) -> Option<Rid> {
        if self.is_empty() {
            return None;
        }
        let curr_page = self
            .buffer_pool_manager
            .fetch_page(self.root_page_id)
            .expect("Root page can not be fetched");
        let mut curr_page_id = curr_page.page_id;
        let mut curr_page =
            BPlusTreePage::from_bytes(&curr_page.data, &self.index_metadata.key_schema);

        // 找到leaf page
        loop {
            match curr_page {
                BPlusTreePage::Internal(internal_page) => {
                    let next_page_id = internal_page.look_up(key, &self.index_metadata.key_schema);
                    let next_page = self
                        .buffer_pool_manager
                        .fetch_page(next_page_id)
                        .expect("Next page can not be fetched");
                    let next_page =
                        BPlusTreePage::from_bytes(&next_page.data, &self.index_metadata.key_schema);
                    // 释放上一页
                    self.buffer_pool_manager.unpin_page(curr_page_id, false);
                    curr_page_id = next_page_id;
                    curr_page = next_page;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    return leaf_page.look_up(key, &self.index_metadata.key_schema);
                }
            }
        }
    }
}
