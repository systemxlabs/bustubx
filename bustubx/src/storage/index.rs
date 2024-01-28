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
        // TODO 计算页容量是否能存放下这么多的kv对
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
            .fetch_page_mut(leaf_page_id)
            .expect("Leaf page can not be fetched");
        let mut leaf_page =
            BPlusTreeLeafPage::from_bytes(&page.data, &self.index_metadata.key_schema);
        leaf_page.insert(key.clone(), rid, &self.index_metadata.key_schema);

        let mut curr_page = BPlusTreePage::Leaf(leaf_page);
        let mut curr_page_id = leaf_page_id;

        // leaf page已满则分裂
        // TODO 可以考虑先分裂再插入，防止越界，可以更多地利用空间
        while curr_page.is_full() {
            // 向右分裂出一个新page
            let internalkv = self.split(&mut curr_page);
            self.buffer_pool_manager
                .write_page(curr_page_id, curr_page.to_bytes());
            self.buffer_pool_manager.unpin_page(curr_page_id, true);

            if let Some(page_id) = context.read_set.pop_back() {
                // 更新父节点
                let page = self
                    .buffer_pool_manager
                    .fetch_page_mut(page_id)
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
        return true;
    }

    pub fn delete(&mut self, key: &Tuple) {
        if self.is_empty() {
            return;
        }
        let mut context = Context::new(self.root_page_id);
        // 找到leaf page
        let leaf_page_id = self.find_leaf_page(key, &mut context);
        let page = self
            .buffer_pool_manager
            .fetch_page_mut(leaf_page_id)
            .expect("Leaf page can not be fetched");
        let mut leaf_page =
            BPlusTreeLeafPage::from_bytes(&page.data, &self.index_metadata.key_schema);
        leaf_page.delete(key, &self.index_metadata.key_schema);

        let mut curr_page = BPlusTreePage::Leaf(leaf_page);
        let mut curr_page_id = leaf_page_id;

        // leaf page未达到半满则从兄弟节点借一个或合并
        while curr_page.is_underflow(self.root_page_id == curr_page_id) {
            if let Some(parent_page_id) = context.read_set.pop_back() {
                let (left_sibling_page_id, right_sibling_page_id) =
                    self.find_sibling_pages(parent_page_id, curr_page_id);

                // 尝试从左兄弟借一个
                if let Some(left_sibling_page_id) = left_sibling_page_id {
                    let left_sibling_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(left_sibling_page_id)
                        .expect("Left sibling page can not be fetched");
                    let mut left_sibling_tree_page = BPlusTreePage::from_bytes(
                        &left_sibling_page.data,
                        &self.index_metadata.key_schema,
                    );
                    if left_sibling_tree_page.can_borrow() {
                        // 从左兄弟借一个，返回父节点需要更新的key
                        let (old_internal_key, new_internal_key) = match left_sibling_tree_page {
                            BPlusTreePage::Internal(ref mut left_sibling_internal_page) => {
                                let kv = left_sibling_internal_page
                                    .split_off(left_sibling_internal_page.current_size as usize - 1)
                                    .remove(0);
                                if let BPlusTreePage::Internal(ref mut curr_internal_page) =
                                    curr_page
                                {
                                    curr_internal_page.insert(
                                        kv.0.clone(),
                                        kv.1,
                                        &self.index_metadata.key_schema,
                                    );
                                    let max_leaf_kv =
                                        self.find_max_leafkv(left_sibling_internal_page.value_at(
                                            left_sibling_internal_page.current_size as usize - 1,
                                        ));
                                    (kv.0, max_leaf_kv.0)
                                } else {
                                    panic!("Leaf page can not borrow from internal page");
                                }
                            }
                            BPlusTreePage::Leaf(ref mut left_sibling_leaf_page) => {
                                let kv = left_sibling_leaf_page
                                    .split_off(left_sibling_leaf_page.current_size as usize - 1)
                                    .remove(0);
                                if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                    curr_leaf_page.insert(
                                        kv.0.clone(),
                                        kv.1,
                                        &self.index_metadata.key_schema,
                                    );
                                    (
                                        kv.0,
                                        left_sibling_leaf_page
                                            .key_at(
                                                left_sibling_leaf_page.current_size as usize - 1,
                                            )
                                            .clone(),
                                    )
                                } else {
                                    panic!("Internal page can not borrow from leaf page");
                                }
                            }
                        };
                        // 更新兄弟节点
                        self.buffer_pool_manager
                            .write_page(left_sibling_page_id, left_sibling_tree_page.to_bytes());
                        self.buffer_pool_manager
                            .unpin_page(left_sibling_page_id, true);

                        // 更新父节点
                        let parent_page = self
                            .buffer_pool_manager
                            .fetch_page_mut(parent_page_id)
                            .expect("Parent page can not be fetched");
                        let mut parent_internal_page = BPlusTreeInternalPage::from_bytes(
                            &parent_page.data,
                            &self.index_metadata.key_schema,
                        );
                        parent_internal_page.replace_key(
                            &old_internal_key,
                            new_internal_key,
                            &self.index_metadata.key_schema,
                        );
                        parent_page.data = parent_internal_page.to_bytes();
                        self.buffer_pool_manager.unpin_page(parent_page_id, true);

                        break;
                    }
                    self.buffer_pool_manager
                        .unpin_page(left_sibling_page_id, false);
                }

                // 尝试从右兄弟借一个
                if let Some(right_sibling_page_id) = right_sibling_page_id {
                    let right_sibling_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(right_sibling_page_id)
                        .expect("Right sibling page can not be fetched");
                    let mut right_sibling_tree_page = BPlusTreePage::from_bytes(
                        &right_sibling_page.data,
                        &self.index_metadata.key_schema,
                    );
                    if right_sibling_tree_page.can_borrow() {
                        // 从右兄弟借一个，返回父节点需要更新的key
                        let (old_internal_key, new_internal_key) = match right_sibling_tree_page {
                            BPlusTreePage::Internal(ref mut right_sibling_internal_page) => {
                                let kv = right_sibling_internal_page.reverse_split_off(0).remove(0);
                                if let BPlusTreePage::Internal(ref mut curr_internal_page) =
                                    curr_page
                                {
                                    curr_internal_page.insert(
                                        kv.0.clone(),
                                        kv.1,
                                        &self.index_metadata.key_schema,
                                    );
                                    let min_leaf_kv = self
                                        .find_min_leafkv(right_sibling_internal_page.value_at(0));
                                    (kv.0, min_leaf_kv.0)
                                } else {
                                    panic!("Leaf page can not borrow from internal page");
                                }
                            }
                            BPlusTreePage::Leaf(ref mut right_sibling_leaf_page) => {
                                let kv = right_sibling_leaf_page.reverse_split_off(0).remove(0);
                                if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                    curr_leaf_page.insert(
                                        kv.0.clone(),
                                        kv.1,
                                        &self.index_metadata.key_schema,
                                    );
                                    (kv.0, right_sibling_leaf_page.key_at(0).clone())
                                } else {
                                    panic!("Internal page can not borrow from leaf page");
                                }
                            }
                        };
                        // 更新兄弟节点
                        self.buffer_pool_manager
                            .write_page(right_sibling_page_id, right_sibling_tree_page.to_bytes());
                        self.buffer_pool_manager
                            .unpin_page(right_sibling_page_id, true);

                        // 更新父节点
                        let parent_page = self
                            .buffer_pool_manager
                            .fetch_page_mut(parent_page_id)
                            .expect("Parent page can not be fetched");
                        let mut parent_internal_page = BPlusTreeInternalPage::from_bytes(
                            &parent_page.data,
                            &self.index_metadata.key_schema,
                        );
                        parent_internal_page.replace_key(
                            &old_internal_key,
                            new_internal_key,
                            &self.index_metadata.key_schema,
                        );
                        parent_page.data = parent_internal_page.to_bytes();
                        self.buffer_pool_manager.unpin_page(parent_page_id, true);

                        break;
                    }
                    self.buffer_pool_manager
                        .unpin_page(right_sibling_page_id, false);
                }

                // 跟左兄弟合并
                if let Some(left_sibling_page_id) = left_sibling_page_id {
                    let left_sibling_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(left_sibling_page_id)
                        .expect("Left sibling page can not be fetched");
                    let mut left_sibling_tree_page = BPlusTreePage::from_bytes(
                        &left_sibling_page.data,
                        &self.index_metadata.key_schema,
                    );
                    // 将当前页向左兄弟合入
                    match left_sibling_tree_page {
                        BPlusTreePage::Internal(ref mut left_sibling_internal_page) => {
                            if let BPlusTreePage::Internal(ref mut curr_internal_page) = curr_page {
                                // 空key处理
                                let mut kvs = curr_internal_page.array.clone();
                                let min_leaf_kv =
                                    self.find_min_leafkv(curr_internal_page.value_at(0));
                                kvs[0].0 = min_leaf_kv.0;
                                left_sibling_internal_page
                                    .batch_insert(kvs, &self.index_metadata.key_schema);
                            } else {
                                panic!("Leaf page can not merge from internal page");
                            }
                        }
                        BPlusTreePage::Leaf(ref mut left_sibling_leaf_page) => {
                            if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                left_sibling_leaf_page.batch_insert(
                                    curr_leaf_page.array.clone(),
                                    &self.index_metadata.key_schema,
                                );
                                // 更新next page id
                                left_sibling_leaf_page.next_page_id = curr_leaf_page.next_page_id;
                            } else {
                                panic!("Internal page can not merge from leaf page");
                            }
                        }
                    };
                    self.buffer_pool_manager
                        .write_page(left_sibling_page_id, left_sibling_tree_page.to_bytes());

                    // 删除当前页
                    let deleted_page_id = curr_page_id;
                    self.buffer_pool_manager.unpin_page(deleted_page_id, false);
                    self.buffer_pool_manager.delete_page(deleted_page_id);

                    // 更新当前页为左兄弟页
                    curr_page_id = left_sibling_page_id;
                    curr_page = left_sibling_tree_page;

                    // 更新父节点
                    let parent_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(parent_page_id)
                        .expect("Parent page can not be fetched");
                    let mut parent_internal_page = BPlusTreeInternalPage::from_bytes(
                        &parent_page.data,
                        &self.index_metadata.key_schema,
                    );
                    parent_internal_page.delete_page_id(deleted_page_id);
                    // 根节点只有一个子节点（叶子）时，则叶子节点成为新的根节点
                    if parent_page_id == self.root_page_id && parent_internal_page.current_size == 0
                    {
                        self.root_page_id = curr_page_id;
                        // 删除旧的根节点
                        self.buffer_pool_manager.unpin_page(parent_page_id, false);
                        self.buffer_pool_manager.delete_page(parent_page_id);
                    } else {
                        parent_page.data = parent_internal_page.to_bytes();
                        self.buffer_pool_manager.unpin_page(curr_page_id, true);
                        curr_page = BPlusTreePage::Internal(parent_internal_page);
                        curr_page_id = parent_page_id;
                    }
                    continue;
                }

                // 跟右兄弟合并
                if let Some(right_sibling_page_id) = right_sibling_page_id {
                    let right_sibling_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(right_sibling_page_id)
                        .expect("Right sibling page can not be fetched");
                    let mut right_sibling_tree_page = BPlusTreePage::from_bytes(
                        &right_sibling_page.data,
                        &self.index_metadata.key_schema,
                    );
                    // 将右兄弟合入当前页
                    match right_sibling_tree_page {
                        BPlusTreePage::Internal(ref mut right_sibling_internal_page) => {
                            if let BPlusTreePage::Internal(ref mut curr_internal_page) = curr_page {
                                // 空key处理
                                let mut kvs = right_sibling_internal_page.array.clone();
                                let min_leaf_kv =
                                    self.find_min_leafkv(right_sibling_internal_page.value_at(0));
                                kvs[0].0 = min_leaf_kv.0;
                                curr_internal_page
                                    .batch_insert(kvs, &self.index_metadata.key_schema);
                            } else {
                                panic!("Leaf page can not merge from internal page");
                            }
                        }
                        BPlusTreePage::Leaf(ref mut right_sibling_leaf_page) => {
                            if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                curr_leaf_page.batch_insert(
                                    right_sibling_leaf_page.array.clone(),
                                    &self.index_metadata.key_schema,
                                );
                                // 更新next page id
                                curr_leaf_page.next_page_id = right_sibling_leaf_page.next_page_id;
                            } else {
                                panic!("Internal page can not merge from leaf page");
                            }
                        }
                    };
                    self.buffer_pool_manager
                        .write_page(curr_page_id, curr_page.to_bytes());

                    // 删除右兄弟页
                    let deleted_page_id = right_sibling_page_id;
                    self.buffer_pool_manager.unpin_page(deleted_page_id, false);
                    self.buffer_pool_manager.delete_page(deleted_page_id);

                    // 更新父节点
                    let parent_page = self
                        .buffer_pool_manager
                        .fetch_page_mut(parent_page_id)
                        .expect("Parent page can not be fetched");
                    let mut parent_internal_page = BPlusTreeInternalPage::from_bytes(
                        &parent_page.data,
                        &self.index_metadata.key_schema,
                    );
                    parent_internal_page.delete_page_id(deleted_page_id);
                    // 根节点只有一个子节点（叶子）时，则叶子节点成为新的根节点
                    if parent_page_id == self.root_page_id && parent_internal_page.current_size == 0
                    {
                        self.root_page_id = curr_page_id;
                        // 删除旧的根节点
                        self.buffer_pool_manager.unpin_page(parent_page_id, false);
                        self.buffer_pool_manager.delete_page(parent_page_id);
                    } else {
                        parent_page.data = parent_internal_page.to_bytes();
                        self.buffer_pool_manager.unpin_page(curr_page_id, true);
                        curr_page = BPlusTreePage::Internal(parent_internal_page);
                        curr_page_id = parent_page_id;
                    }
                    continue;
                }
            }
        }

        self.buffer_pool_manager
            .write_page(curr_page_id, curr_page.to_bytes());
        self.buffer_pool_manager.unpin_page(curr_page_id, true);
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
            .fetch_page_mut(leaf_page_id)
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

    // 分裂page
    fn split(&mut self, page: &mut BPlusTreePage) -> InternalKV {
        match page {
            BPlusTreePage::Leaf(leaf_page) => {
                let new_page = self
                    .buffer_pool_manager
                    .new_page()
                    .expect("failed to split leaf page");
                let new_page_id = new_page.page_id;

                // 拆分kv对
                let mut new_leaf_page = BPlusTreeLeafPage::new(self.leaf_max_size as u32);
                new_leaf_page.batch_insert(
                    leaf_page.split_off(leaf_page.current_size as usize / 2),
                    &self.index_metadata.key_schema,
                );

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
                new_internal_page.batch_insert(
                    internal_page.split_off(internal_page.current_size as usize / 2),
                    &self.index_metadata.key_schema,
                );

                new_page.data = new_internal_page.to_bytes();
                self.buffer_pool_manager.unpin_page(new_page_id, true);

                let min_leafkv = self.find_min_leafkv(new_page_id);
                return (min_leafkv.0, new_page_id);
            }
        }
    }

    fn borrow(&mut self, page: &mut BPlusTreePage, context: &mut Context) {
        unimplemented!()
    }

    fn find_sibling_pages(
        &mut self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> (Option<PageId>, Option<PageId>) {
        let parent_page = self
            .buffer_pool_manager
            .fetch_page(parent_page_id)
            .expect("Parent page can not be fetched");
        let parent_page =
            BPlusTreeInternalPage::from_bytes(&parent_page.data, &self.index_metadata.key_schema);
        self.buffer_pool_manager.unpin_page(parent_page_id, false);
        return parent_page.sibling_page_ids(child_page_id);
    }

    fn merge(&mut self, page: &BPlusTreePage, context: &mut Context) {
        unimplemented!()
    }

    // 查找子树最小的leafKV
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

    // 查找子树最大的leafKV
    fn find_max_leafkv(&mut self, page_id: PageId) -> LeafKV {
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
                    let page_id = internal_page.value_at(internal_page.current_size as usize - 1);
                    let page = self
                        .buffer_pool_manager
                        .fetch_page(page_id)
                        .expect("Page can not be fetched");
                    curr_page =
                        BPlusTreePage::from_bytes(&page.data, &self.index_metadata.key_schema);
                    self.buffer_pool_manager.unpin_page(page_id, false);
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    return leaf_page.kv_at(leaf_page.current_size as usize - 1).clone();
                }
            }
        }
    }

    pub fn print_tree(&mut self) {
        if self.is_empty() {
            println!("Empty tree.");
            return;
        }
        // 层序遍历
        let mut curr_queue = VecDeque::new();
        curr_queue.push_back(self.root_page_id);

        let mut level_index = 1;
        loop {
            if curr_queue.is_empty() {
                break;
            }
            let mut next_queue = VecDeque::new();
            // 打印当前层
            println!("B+树第{}层: ", level_index);
            while let Some(page_id) = curr_queue.pop_front() {
                let page = self
                    .buffer_pool_manager
                    .fetch_page(page_id)
                    .expect("Page can not be fetched");
                let curr_page =
                    BPlusTreePage::from_bytes(&page.data, &self.index_metadata.key_schema);
                self.buffer_pool_manager.unpin_page(page_id, false);
                match curr_page {
                    BPlusTreePage::Internal(internal_page) => {
                        internal_page.print_page(page_id, &self.index_metadata.key_schema);
                        println!();
                        next_queue.extend(internal_page.values());
                    }
                    BPlusTreePage::Leaf(leaf_page) => {
                        leaf_page.print_page(page_id, &self.index_metadata.key_schema);
                        println!();
                    }
                }
            }
            println!();
            level_index += 1;
            curr_queue = next_queue;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, sync::Arc};

    use crate::{
        buffer::buffer_pool,
        catalog::{column::Column, data_type::DataType, schema::Schema},
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
                Column::new("a".to_string(), DataType::Int8),
                Column::new("b".to_string(), DataType::Int16),
                Column::new("c".to_string(), DataType::Int8),
                Column::new("d".to_string(), DataType::Int16),
            ]),
            vec![1, 3],
        );
        assert_eq!(index_metadata.key_schema.column_count(), 2);
        assert_eq!(
            index_metadata.key_schema.get_col_by_index(0).unwrap().name,
            "b"
        );
        assert_eq!(
            index_metadata.key_schema.get_col_by_index(1).unwrap().name,
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
                Column::new("a".to_string(), DataType::Int8),
                Column::new("b".to_string(), DataType::Int16),
            ]),
            vec![0, 1],
        );
        let disk_manager = disk_manager::DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = buffer_pool::BufferPoolManager::new(1000, Arc::new(disk_manager));
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
        assert_eq!(
            index.get(&Tuple::new(vec![5, 5, 5])).unwrap(),
            Rid::new(5, 5)
        );
        assert_eq!(index.root_page_id, 6);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 7);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_index_delete() {
        let db_path = "./test_index_delete.db";
        let _ = remove_file(db_path);

        let index_metadata = IndexMetadata::new(
            "test_index".to_string(),
            "test_table".to_string(),
            &Schema::new(vec![
                Column::new("a".to_string(), DataType::Int8),
                Column::new("b".to_string(), DataType::Int16),
            ]),
            vec![0, 1],
        );
        let disk_manager = disk_manager::DiskManager::try_new(&db_path).unwrap();
        let buffer_pool_manager = buffer_pool::BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut index = BPlusTreeIndex::new(index_metadata, buffer_pool_manager, 4, 5);

        index.insert(&Tuple::new(vec![1, 1, 1]), Rid::new(1, 1));
        index.insert(&Tuple::new(vec![2, 2, 2]), Rid::new(2, 2));
        index.insert(&Tuple::new(vec![3, 3, 3]), Rid::new(3, 3));
        index.insert(&Tuple::new(vec![4, 4, 4]), Rid::new(4, 4));
        index.insert(&Tuple::new(vec![5, 5, 5]), Rid::new(5, 5));
        index.insert(&Tuple::new(vec![6, 6, 6]), Rid::new(6, 6));
        index.insert(&Tuple::new(vec![7, 7, 7]), Rid::new(7, 7));
        index.insert(&Tuple::new(vec![8, 8, 8]), Rid::new(8, 8));
        index.insert(&Tuple::new(vec![9, 9, 9]), Rid::new(9, 9));
        index.insert(&Tuple::new(vec![10, 10, 10]), Rid::new(10, 10));
        assert_eq!(index.buffer_pool_manager.replacer.size(), 5);
        assert_eq!(index.root_page_id, 2);
        index.print_tree();

        index.delete(&Tuple::new(vec![1, 1, 1]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![1, 1, 1])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 4);

        index.delete(&Tuple::new(vec![3, 3, 3]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![3, 3, 3])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 4);

        index.delete(&Tuple::new(vec![5, 5, 5]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![5, 5, 5])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 4);

        index.delete(&Tuple::new(vec![7, 7, 7]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![7, 7, 7])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 4);

        index.delete(&Tuple::new(vec![9, 9, 9]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![9, 9, 9])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 3);

        index.delete(&Tuple::new(vec![10, 10, 10]));
        assert_eq!(index.root_page_id, 2);
        assert_eq!(index.get(&Tuple::new(vec![10, 10, 10])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 3);

        index.delete(&Tuple::new(vec![8, 8, 8]));
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.get(&Tuple::new(vec![8, 8, 8])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);

        index.delete(&Tuple::new(vec![6, 6, 6]));
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.get(&Tuple::new(vec![6, 6, 6])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);

        index.delete(&Tuple::new(vec![4, 4, 4]));
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.get(&Tuple::new(vec![4, 4, 4])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);

        index.delete(&Tuple::new(vec![2, 2, 2]));
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.get(&Tuple::new(vec![2, 2, 2])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);

        index.delete(&Tuple::new(vec![2, 2, 2]));
        assert_eq!(index.root_page_id, 0);
        assert_eq!(index.get(&Tuple::new(vec![2, 2, 2])), None);
        assert_eq!(index.buffer_pool_manager.replacer.size(), 1);

        let _ = remove_file(db_path);
    }
}
