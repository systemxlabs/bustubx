use std::collections::VecDeque;

use crate::{
    buffer::buffer_pool::BufferPoolManager,
    catalog::schema::Schema,
    common::{config::INVALID_PAGE_ID, rid::Rid},
    storage::index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage},
};

use super::{
    index_page::{InternalKV, LeafKV},
    page::PageId,
    tuple::Tuple,
};

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
    pub write_set: VecDeque<PageId>,
    pub read_set: VecDeque<PageId>,
}
impl Context {
    pub fn new(root_page_id: PageId) -> Self {
        Self {
            root_page_id,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
        }
    }
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
        let mut context = Context::new(self.root_page_id);
        // 找到leaf page
        let leaf_page_id = self.find_leaf_page(key, &mut context);
        let page = self
            .buffer_pool_manager
            .fetch_page(leaf_page_id)
            .expect("Leaf page can not be fetched");
        let mut leaf_page =
            BPlusTreeLeafPage::from_bytes(&page.data, &self.index_metadata.key_schema);
        leaf_page.insert(key.clone(), rid, &self.index_metadata.key_schema);

        if leaf_page.is_full() {
            // leaf page已满，分裂
            let mut curr_page = BPlusTreePage::Leaf(leaf_page);
            let mut curr_page_id = leaf_page_id;

            while curr_page.is_full() {
                // 向右分裂出一个新page
                let internalkv = self.split(&mut curr_page, &mut context);
                self.buffer_pool_manager
                    .write_page(curr_page_id, curr_page.to_bytes());
                self.buffer_pool_manager.unpin_page(curr_page_id, true);

                if let Some(page_id) = context.read_set.pop_front() {
                    // 更新父节点
                    let page = self
                        .buffer_pool_manager
                        .fetch_page(page_id)
                        .expect("Page can not be fetched");
                    let mut tree_page =
                        BPlusTreePage::from_bytes(&page.data, &self.index_metadata.key_schema);
                    self.buffer_pool_manager.unpin_page(page_id, false);
                    tree_page.insert_internalkv(internalkv, &self.index_metadata.key_schema);

                    curr_page = tree_page;
                    curr_page_id = page_id;
                } else if curr_page_id == self.root_page_id {
                    // new 一个新的root page
                    let new_root_page = self
                        .buffer_pool_manager
                        .new_page()
                        .expect("can not new root page");
                    let new_root_page_id = new_root_page.page_id;
                    let mut new_internal_page =
                        BPlusTreeInternalPage::new(self.internal_max_size as u32);

                    // internal page第一个kv对的key为空
                    new_internal_page.insert(
                        Tuple::empty(self.index_metadata.key_schema.fixed_len()),
                        self.root_page_id,
                        &self.index_metadata.key_schema,
                    );
                    new_internal_page.insert(
                        internalkv.0,
                        internalkv.1,
                        &self.index_metadata.key_schema,
                    );
                    new_root_page.data = new_internal_page.to_bytes();
                    self.buffer_pool_manager.unpin_page(new_root_page_id, true);

                    // 更新root page id
                    self.root_page_id = new_root_page_id;

                    curr_page = BPlusTreePage::Internal(new_internal_page);
                    curr_page_id = new_root_page_id;
                }
            }

            self.buffer_pool_manager
                .write_page(curr_page_id, curr_page.to_bytes());
            self.buffer_pool_manager.unpin_page(curr_page_id, true);
        } else {
            page.data = leaf_page.to_bytes();
            self.buffer_pool_manager.unpin_page(leaf_page_id, true);
        }
        return true;
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

        // 更新root page id
        self.root_page_id = new_page_id;

        self.buffer_pool_manager.unpin_page(new_page_id, true);
    }

    // 找到叶子节点上对应的Value
    pub fn get(&mut self, key: &Tuple) -> Option<Rid> {
        if self.is_empty() {
            return None;
        }

        // 找到leaf page
        let mut context = Context::new(self.root_page_id);
        let leaf_page_id = self.find_leaf_page(key, &mut context);
        if leaf_page_id == INVALID_PAGE_ID {
            return None;
        }

        let leaf_page = self
            .buffer_pool_manager
            .fetch_page(leaf_page_id)
            .expect("Leaf page can not be fetched");
        let leaf_page =
            BPlusTreeLeafPage::from_bytes(&leaf_page.data, &self.index_metadata.key_schema);
        let result = leaf_page.look_up(key, &self.index_metadata.key_schema);
        self.buffer_pool_manager.unpin_page(leaf_page_id, false);
        return result;
    }

    fn find_leaf_page(&mut self, key: &Tuple, context: &mut Context) -> PageId {
        if self.is_empty() {
            return INVALID_PAGE_ID;
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
                    context.read_set.push_back(curr_page_id);
                    // 释放上一页
                    self.buffer_pool_manager.unpin_page(curr_page_id, false);
                    // 查找下一页
                    let next_page_id = internal_page.look_up(key, &self.index_metadata.key_schema);
                    let next_page = self
                        .buffer_pool_manager
                        .fetch_page(next_page_id)
                        .expect("Next page can not be fetched");
                    let next_page =
                        BPlusTreePage::from_bytes(&next_page.data, &self.index_metadata.key_schema);
                    curr_page_id = next_page_id;
                    curr_page = next_page;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    self.buffer_pool_manager.unpin_page(curr_page_id, false);
                    return curr_page_id;
                }
            }
        }
    }

    fn split(&mut self, page: &mut BPlusTreePage, context: &mut Context) -> InternalKV {
        match page {
            BPlusTreePage::Leaf(leaf_page) => {
                let new_page = self
                    .buffer_pool_manager
                    .new_page()
                    .expect("failed to split leaf page");
                let new_page_id = new_page.page_id;

                // 拆分kv对
                let mut new_leaf_page = BPlusTreeLeafPage::new(self.leaf_max_size as u32);
                new_leaf_page.batch_insert(leaf_page.split_off(), &self.index_metadata.key_schema);

                // 更新next page id
                new_leaf_page.next_page_id = leaf_page.next_page_id;
                leaf_page.next_page_id = new_page.page_id;

                new_page.data = new_leaf_page.to_bytes();
                self.buffer_pool_manager.unpin_page(new_page_id, true);

                return (new_leaf_page.key_at(0).clone(), new_page_id);
            }
            BPlusTreePage::Internal(internal_page) => {
                let new_page = self
                    .buffer_pool_manager
                    .new_page()
                    .expect("failed to split internal page");
                let new_page_id = new_page.page_id;

                // 拆分kv对
                let mut new_internal_page =
                    BPlusTreeInternalPage::new(self.internal_max_size as u32);
                new_internal_page
                    .batch_insert(internal_page.split_off(), &self.index_metadata.key_schema);

                new_page.data = new_internal_page.to_bytes();
                self.buffer_pool_manager.unpin_page(new_page_id, true);

                let min_leafkv = self.find_min_leafkv(new_page_id);
                return (min_leafkv.0, new_page_id);
            }
        }
    }

    fn merge(&mut self, page: &BPlusTreePage, context: &mut Context) {
        unimplemented!()
    }

    fn find_min_leafkv(&mut self, page_id: PageId) -> LeafKV {
        let curr_page = self
            .buffer_pool_manager
            .fetch_page(page_id)
            .expect("Page can not be fetched");
        let mut curr_page =
            BPlusTreePage::from_bytes(&curr_page.data, &self.index_metadata.key_schema);
        self.buffer_pool_manager.unpin_page(page_id, false);
        loop {
            match curr_page {
                BPlusTreePage::Internal(internal_page) => {
                    let page_id = internal_page.value_at(0);
                    let page = self
                        .buffer_pool_manager
                        .fetch_page(page_id)
                        .expect("Page can not be fetched");
                    curr_page =
                        BPlusTreePage::from_bytes(&page.data, &self.index_metadata.key_schema);
                    self.buffer_pool_manager.unpin_page(page_id, false);
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    return leaf_page.kv_at(0).clone();
                }
            }
        }
    }
}

mod tests {
    use std::fs::remove_file;

    use crate::{
        buffer::buffer_pool,
        catalog::{
            column::{Column, DataType},
            schema::Schema,
        },
        common::{config::INVALID_PAGE_ID, rid::Rid},
        storage::{disk_manager, tuple::Tuple},
    };

    use super::{BPlusTreeIndex, IndexMetadata};

    #[test]
    pub fn test_index_metadata_new() {
        let index_metadata = IndexMetadata::new(
            "test_index".to_string(),
            "test_table".to_string(),
            &Schema::new(vec![
                Column::new("a".to_string(), DataType::TinyInt, 0),
                Column::new("b".to_string(), DataType::SmallInt, 0),
                Column::new("c".to_string(), DataType::TinyInt, 0),
                Column::new("d".to_string(), DataType::SmallInt, 0),
            ]),
            vec![1, 3],
        );
        assert_eq!(index_metadata.key_schema.column_count(), 2);
        assert_eq!(
            index_metadata
                .key_schema
                .get_by_index(0)
                .unwrap()
                .column_name,
            "b"
        );
        assert_eq!(
            index_metadata
                .key_schema
                .get_by_index(1)
                .unwrap()
                .column_name,
            "d"
        );
    }

    #[test]
    pub fn test_index_insert() {
        let db_path = "./test_index_insert.db";
        let _ = remove_file(db_path);

        let index_metadata = IndexMetadata::new(
            "test_index".to_string(),
            "test_table".to_string(),
            &Schema::new(vec![
                Column::new("a".to_string(), DataType::TinyInt, 0),
                Column::new("b".to_string(), DataType::SmallInt, 0),
            ]),
            vec![0, 1],
        );
        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = buffer_pool::BufferPoolManager::new(1000, disk_manager);
        let mut index = BPlusTreeIndex::new(index_metadata, buffer_pool_manager, 2, 3);

        index.insert(&Tuple::new(vec![1, 1, 1]), Rid::new(1, 1));
        assert_eq!(
            index.get(&Tuple::new(vec![1, 1, 1])).unwrap(),
            Rid::new(1, 1)
        );
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);
        index.insert(&Tuple::new(vec![2, 2, 2]), Rid::new(2, 2));
        assert_eq!(
            index.get(&Tuple::new(vec![2, 2, 2])).unwrap(),
            Rid::new(2, 2)
        );
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);
        index.insert(&Tuple::new(vec![3, 3, 3]), Rid::new(3, 3));
        assert_eq!(
            index.get(&Tuple::new(vec![3, 3, 3])).unwrap(),
            Rid::new(3, 3)
        );
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 3);
        index.insert(&Tuple::new(vec![4, 4, 4]), Rid::new(4, 4));
        assert_eq!(
            index.get(&Tuple::new(vec![4, 4, 4])).unwrap(),
            Rid::new(4, 4)
        );
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 4);
        index.insert(&Tuple::new(vec![5, 5, 5]), Rid::new(5, 5));
        assert_eq!(index.buffer_pool_manager.replacer.size(), 7);
        assert_eq!(
            index.get(&Tuple::new(vec![5, 5, 5])).unwrap(),
            Rid::new(5, 5)
        );
        assert_eq!(index.root_page_id, 6);

        let _ = remove_file(db_path);
    }
}
