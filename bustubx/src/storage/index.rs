use comfy_table::{Cell, ContentArrangement};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use crate::buffer::{Page, PageId, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::common::util::page_bytes_to_array;
use crate::storage::codec::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec,
};
use crate::storage::{InternalKV, LeafKV};
use crate::{
    buffer::BufferPoolManager,
    common::rid::Rid,
    storage::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage},
    BustubxError, BustubxResult,
};

use super::tuple::Tuple;

struct Context {
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
    pub key_schema: SchemaRef,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub leaf_max_size: u32,
    pub internal_max_size: u32,
    pub root_page_id: PageId,
}

impl BPlusTreeIndex {
    pub fn new(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferPoolManager>,
        leaf_max_size: u32,
        internal_max_size: u32,
    ) -> Self {
        Self {
            key_schema,
            buffer_pool,
            leaf_max_size,
            internal_max_size,
            root_page_id: INVALID_PAGE_ID,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id == INVALID_PAGE_ID
    }

    pub fn insert(&mut self, key: &Tuple, rid: Rid) -> BustubxResult<()> {
        if self.is_empty() {
            self.start_new_tree(key, rid)?;
            return Ok(());
        }
        let mut context = Context::new(self.root_page_id);
        // 找到leaf page
        let Some(leaf_page) = self.find_leaf_page(key, &mut context)? else {
            return Err(BustubxError::Execution(
                "Cannot find leaf page to insert".to_string(),
            ));
        };

        let (mut leaf_tree_page, _) = BPlusTreeLeafPageCodec::decode(
            &leaf_page.read().unwrap().data,
            self.key_schema.clone(),
        )?;
        leaf_tree_page.insert(key.clone(), rid);

        let mut curr_page = leaf_page;
        let mut curr_tree_page = BPlusTreePage::Leaf(leaf_tree_page);

        // leaf page已满则分裂
        while curr_tree_page.is_full() {
            // 向右分裂出一个新page
            let internalkv = self.split(&mut curr_tree_page)?;

            curr_page.write().unwrap().data =
                page_bytes_to_array(&BPlusTreePageCodec::encode(&curr_tree_page));
            self.buffer_pool.unpin_page(curr_page.clone(), true)?;

            let curr_page_id = curr_page.read().unwrap().page_id;
            if let Some(parent_page_id) = context.read_set.pop_back() {
                // 更新父节点
                let parent_page = self.buffer_pool.fetch_page(parent_page_id)?;
                let (mut parent_tree_page, _) = BPlusTreePageCodec::decode(
                    &parent_page.read().unwrap().data,
                    self.key_schema.clone(),
                )?;
                self.buffer_pool.unpin_page(parent_page.clone(), false)?;
                parent_tree_page.insert_internalkv(internalkv);

                curr_page = parent_page;
                curr_tree_page = parent_tree_page;
            } else if curr_page_id == self.root_page_id {
                // new 一个新的root page
                let new_root_page = self.buffer_pool.new_page()?;
                let new_root_page_id = new_root_page.read().unwrap().page_id;
                let mut new_root_internal_page =
                    BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);

                // internal page第一个kv对的key为空
                new_root_internal_page
                    .insert(Tuple::empty(self.key_schema.clone()), self.root_page_id);
                new_root_internal_page.insert(internalkv.0, internalkv.1);

                new_root_page.write().unwrap().data = page_bytes_to_array(
                    &BPlusTreeInternalPageCodec::encode(&new_root_internal_page),
                );
                self.buffer_pool.unpin_page(new_root_page.clone(), true)?;

                // 更新root page id
                self.root_page_id = new_root_page_id;

                curr_page = new_root_page;
                curr_tree_page = BPlusTreePage::Internal(new_root_internal_page);
            }
        }

        curr_page.write().unwrap().data =
            page_bytes_to_array(&BPlusTreePageCodec::encode(&curr_tree_page));
        self.buffer_pool.unpin_page(curr_page, true)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &Tuple) {
        if self.is_empty() {
            return;
        }
        let mut context = Context::new(self.root_page_id);
        // 找到leaf page
        let Some(leaf_page) = self.find_leaf_page(key, &mut context).unwrap() else {
            todo!()
        };
        let (mut leaf_tree_page, _) = BPlusTreeLeafPageCodec::decode(
            &leaf_page.read().unwrap().data,
            self.key_schema.clone(),
        )
        .unwrap();
        leaf_tree_page.delete(key);

        let mut curr_page = BPlusTreePage::Leaf(leaf_tree_page);
        let mut curr_page_id = leaf_page.read().unwrap().page_id;

        // leaf page未达到半满则从兄弟节点借一个或合并
        while curr_page.is_underflow(self.root_page_id == curr_page_id) {
            if let Some(parent_page_id) = context.read_set.pop_back() {
                let (left_sibling_page_id, right_sibling_page_id) = self
                    .find_sibling_pages(parent_page_id, curr_page_id)
                    .unwrap();

                // 尝试从左兄弟借一个
                if let Some(left_sibling_page_id) = left_sibling_page_id {
                    let left_sibling_page = self
                        .buffer_pool
                        .fetch_page(left_sibling_page_id)
                        .expect("Left sibling page can not be fetched");
                    let (mut left_sibling_tree_page, _) = BPlusTreePageCodec::decode(
                        &left_sibling_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    if left_sibling_tree_page.can_borrow() {
                        // 从左兄弟借一个，返回父节点需要更新的key
                        let (old_internal_key, new_internal_key) = match left_sibling_tree_page {
                            BPlusTreePage::Internal(ref mut left_sibling_internal_page) => {
                                let kv = left_sibling_internal_page
                                    .split_off(
                                        left_sibling_internal_page.header.current_size as usize - 1,
                                    )
                                    .remove(0);
                                if let BPlusTreePage::Internal(ref mut curr_internal_page) =
                                    curr_page
                                {
                                    curr_internal_page.insert(kv.0.clone(), kv.1);
                                    let max_leaf_kv = self
                                        .find_subtree_max_leafkv(
                                            left_sibling_internal_page.value_at(
                                                left_sibling_internal_page.header.current_size
                                                    as usize
                                                    - 1,
                                            ),
                                        )
                                        .unwrap();
                                    (kv.0, max_leaf_kv.0)
                                } else {
                                    panic!("Leaf page can not borrow from internal page");
                                }
                            }
                            BPlusTreePage::Leaf(ref mut left_sibling_leaf_page) => {
                                let kv = left_sibling_leaf_page
                                    .split_off(
                                        left_sibling_leaf_page.header.current_size as usize - 1,
                                    )
                                    .remove(0);
                                if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                    curr_leaf_page.insert(kv.0.clone(), kv.1);
                                    (
                                        kv.0,
                                        left_sibling_leaf_page
                                            .key_at(
                                                left_sibling_leaf_page.header.current_size as usize
                                                    - 1,
                                            )
                                            .clone(),
                                    )
                                } else {
                                    panic!("Internal page can not borrow from leaf page");
                                }
                            }
                        };
                        // 更新兄弟节点
                        self.buffer_pool.write_page(
                            left_sibling_page_id,
                            page_bytes_to_array(&BPlusTreePageCodec::encode(
                                &left_sibling_tree_page,
                            )),
                        );
                        self.buffer_pool
                            .unpin_page_id(left_sibling_page_id, true)
                            .unwrap();

                        // 更新父节点
                        let parent_page = self
                            .buffer_pool
                            .fetch_page(parent_page_id)
                            .expect("Parent page can not be fetched");
                        let (mut parent_internal_page, _) = BPlusTreeInternalPageCodec::decode(
                            &parent_page.read().unwrap().data,
                            self.key_schema.clone(),
                        )
                        .unwrap();
                        parent_internal_page.replace_key(&old_internal_key, new_internal_key);

                        parent_page.write().unwrap().data = page_bytes_to_array(
                            &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
                        );
                        self.buffer_pool
                            .unpin_page_id(parent_page_id, true)
                            .unwrap();

                        break;
                    }
                    self.buffer_pool
                        .unpin_page_id(left_sibling_page_id, false)
                        .unwrap();
                }

                // 尝试从右兄弟借一个
                if let Some(right_sibling_page_id) = right_sibling_page_id {
                    let right_sibling_page = self
                        .buffer_pool
                        .fetch_page(right_sibling_page_id)
                        .expect("Right sibling page can not be fetched");
                    let (mut right_sibling_tree_page, _) = BPlusTreePageCodec::decode(
                        &right_sibling_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    if right_sibling_tree_page.can_borrow() {
                        // 从右兄弟借一个，返回父节点需要更新的key
                        let (old_internal_key, new_internal_key) = match right_sibling_tree_page {
                            BPlusTreePage::Internal(ref mut right_sibling_internal_page) => {
                                let kv = right_sibling_internal_page.reverse_split_off(0).remove(0);
                                if let BPlusTreePage::Internal(ref mut curr_internal_page) =
                                    curr_page
                                {
                                    curr_internal_page.insert(kv.0.clone(), kv.1);
                                    let min_leaf_kv = self
                                        .find_subtree_min_leafkv(
                                            right_sibling_internal_page.value_at(0),
                                        )
                                        .unwrap();
                                    (kv.0, min_leaf_kv.0)
                                } else {
                                    panic!("Leaf page can not borrow from internal page");
                                }
                            }
                            BPlusTreePage::Leaf(ref mut right_sibling_leaf_page) => {
                                let kv = right_sibling_leaf_page.reverse_split_off(0).remove(0);
                                if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                    curr_leaf_page.insert(kv.0.clone(), kv.1);
                                    (kv.0, right_sibling_leaf_page.key_at(0).clone())
                                } else {
                                    panic!("Internal page can not borrow from leaf page");
                                }
                            }
                        };
                        // 更新兄弟节点
                        self.buffer_pool.write_page(
                            right_sibling_page_id,
                            page_bytes_to_array(&BPlusTreePageCodec::encode(
                                &right_sibling_tree_page,
                            )),
                        );
                        self.buffer_pool
                            .unpin_page_id(right_sibling_page_id, true)
                            .unwrap();

                        // 更新父节点
                        let parent_page = self
                            .buffer_pool
                            .fetch_page(parent_page_id)
                            .expect("Parent page can not be fetched");
                        let (mut parent_internal_page, _) = BPlusTreeInternalPageCodec::decode(
                            &parent_page.read().unwrap().data,
                            self.key_schema.clone(),
                        )
                        .unwrap();
                        parent_internal_page.replace_key(&old_internal_key, new_internal_key);

                        parent_page.write().unwrap().data = page_bytes_to_array(
                            &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
                        );
                        self.buffer_pool
                            .unpin_page_id(parent_page_id, true)
                            .unwrap();

                        break;
                    }
                    self.buffer_pool
                        .unpin_page_id(right_sibling_page_id, false)
                        .unwrap();
                }

                // 跟左兄弟合并
                if let Some(left_sibling_page_id) = left_sibling_page_id {
                    let left_sibling_page = self
                        .buffer_pool
                        .fetch_page(left_sibling_page_id)
                        .expect("Left sibling page can not be fetched");
                    let (mut left_sibling_tree_page, _) = BPlusTreePageCodec::decode(
                        &left_sibling_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    // 将当前页向左兄弟合入
                    match left_sibling_tree_page {
                        BPlusTreePage::Internal(ref mut left_sibling_internal_page) => {
                            if let BPlusTreePage::Internal(ref mut curr_internal_page) = curr_page {
                                // 空key处理
                                let mut kvs = curr_internal_page.array.clone();
                                let min_leaf_kv = self
                                    .find_subtree_min_leafkv(curr_internal_page.value_at(0))
                                    .unwrap();
                                kvs[0].0 = min_leaf_kv.0;
                                left_sibling_internal_page.batch_insert(kvs);
                            } else {
                                panic!("Leaf page can not merge from internal page");
                            }
                        }
                        BPlusTreePage::Leaf(ref mut left_sibling_leaf_page) => {
                            if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                left_sibling_leaf_page.batch_insert(curr_leaf_page.array.clone());
                                // 更新next page id
                                left_sibling_leaf_page.header.next_page_id =
                                    curr_leaf_page.header.next_page_id;
                            } else {
                                panic!("Internal page can not merge from leaf page");
                            }
                        }
                    };

                    self.buffer_pool.write_page(
                        left_sibling_page_id,
                        page_bytes_to_array(&BPlusTreePageCodec::encode(&left_sibling_tree_page)),
                    );

                    // 删除当前页
                    let deleted_page_id = curr_page_id;
                    self.buffer_pool
                        .unpin_page_id(deleted_page_id, false)
                        .unwrap();
                    self.buffer_pool.delete_page(deleted_page_id).unwrap();

                    // 更新当前页为左兄弟页
                    curr_page_id = left_sibling_page_id;
                    curr_page = left_sibling_tree_page;

                    // 更新父节点
                    let parent_page = self
                        .buffer_pool
                        .fetch_page(parent_page_id)
                        .expect("Parent page can not be fetched");
                    let (mut parent_internal_page, _) = BPlusTreeInternalPageCodec::decode(
                        &parent_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    parent_internal_page.delete_page_id(deleted_page_id);
                    // 根节点只有一个子节点（叶子）时，则叶子节点成为新的根节点
                    if parent_page_id == self.root_page_id
                        && parent_internal_page.header.current_size == 0
                    {
                        self.root_page_id = curr_page_id;
                        // 删除旧的根节点
                        self.buffer_pool
                            .unpin_page_id(parent_page_id, false)
                            .unwrap();
                        self.buffer_pool.delete_page(parent_page_id).unwrap();
                    } else {
                        parent_page.write().unwrap().data = page_bytes_to_array(
                            &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
                        );
                        self.buffer_pool.unpin_page_id(curr_page_id, true).unwrap();
                        curr_page = BPlusTreePage::Internal(parent_internal_page);
                        curr_page_id = parent_page_id;
                    }
                    continue;
                }

                // 跟右兄弟合并
                if let Some(right_sibling_page_id) = right_sibling_page_id {
                    let right_sibling_page = self
                        .buffer_pool
                        .fetch_page(right_sibling_page_id)
                        .expect("Right sibling page can not be fetched");
                    let (mut right_sibling_tree_page, _) = BPlusTreePageCodec::decode(
                        &right_sibling_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    // 将右兄弟合入当前页
                    match right_sibling_tree_page {
                        BPlusTreePage::Internal(ref mut right_sibling_internal_page) => {
                            if let BPlusTreePage::Internal(ref mut curr_internal_page) = curr_page {
                                // 空key处理
                                let mut kvs = right_sibling_internal_page.array.clone();
                                let min_leaf_kv = self
                                    .find_subtree_min_leafkv(
                                        right_sibling_internal_page.value_at(0),
                                    )
                                    .unwrap();
                                kvs[0].0 = min_leaf_kv.0;
                                curr_internal_page.batch_insert(kvs);
                            } else {
                                panic!("Leaf page can not merge from internal page");
                            }
                        }
                        BPlusTreePage::Leaf(ref mut right_sibling_leaf_page) => {
                            if let BPlusTreePage::Leaf(ref mut curr_leaf_page) = curr_page {
                                curr_leaf_page.batch_insert(right_sibling_leaf_page.array.clone());
                                // 更新next page id
                                curr_leaf_page.header.next_page_id =
                                    right_sibling_leaf_page.header.next_page_id;
                            } else {
                                panic!("Internal page can not merge from leaf page");
                            }
                        }
                    };

                    self.buffer_pool.write_page(
                        curr_page_id,
                        page_bytes_to_array(&BPlusTreePageCodec::encode(&curr_page)),
                    );

                    // 删除右兄弟页
                    let deleted_page_id = right_sibling_page_id;
                    self.buffer_pool
                        .unpin_page_id(deleted_page_id, false)
                        .unwrap();
                    self.buffer_pool.delete_page(deleted_page_id).unwrap();

                    // 更新父节点
                    let parent_page = self
                        .buffer_pool
                        .fetch_page(parent_page_id)
                        .expect("Parent page can not be fetched");
                    let (mut parent_internal_page, _) = BPlusTreeInternalPageCodec::decode(
                        &parent_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )
                    .unwrap();
                    parent_internal_page.delete_page_id(deleted_page_id);
                    // 根节点只有一个子节点（叶子）时，则叶子节点成为新的根节点
                    if parent_page_id == self.root_page_id
                        && parent_internal_page.header.current_size == 0
                    {
                        self.root_page_id = curr_page_id;
                        // 删除旧的根节点
                        self.buffer_pool
                            .unpin_page_id(parent_page_id, false)
                            .unwrap();
                        self.buffer_pool.delete_page(parent_page_id).unwrap();
                    } else {
                        parent_page.write().unwrap().data = page_bytes_to_array(
                            &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
                        );
                        self.buffer_pool.unpin_page_id(curr_page_id, true).unwrap();
                        curr_page = BPlusTreePage::Internal(parent_internal_page);
                        curr_page_id = parent_page_id;
                    }
                    continue;
                }
            }
        }

        self.buffer_pool.write_page(
            curr_page_id,
            page_bytes_to_array(&BPlusTreePageCodec::encode(&curr_page)),
        );
        self.buffer_pool.unpin_page_id(curr_page_id, true).unwrap();
    }

    pub fn scan(&self, _key: &Tuple) -> Vec<Rid> {
        unimplemented!()
    }

    fn start_new_tree(&mut self, key: &Tuple, rid: Rid) -> BustubxResult<()> {
        let new_page = self.buffer_pool.new_page()?;
        let new_page_id = new_page.read().unwrap().page_id;

        let mut leaf_page = BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
        leaf_page.insert(key.clone(), rid);

        new_page.write().unwrap().data =
            page_bytes_to_array(&BPlusTreeLeafPageCodec::encode(&leaf_page));

        // 更新root page id
        self.root_page_id = new_page_id;

        self.buffer_pool.unpin_page_id(new_page_id, true)?;
        Ok(())
    }

    // 找到叶子节点上对应的Value
    pub fn get(&mut self, key: &Tuple) -> BustubxResult<Option<Rid>> {
        if self.is_empty() {
            return Ok(None);
        }

        // 找到leaf page
        let mut context = Context::new(self.root_page_id);
        let Some(leaf_page) = self.find_leaf_page(key, &mut context)? else {
            return Ok(None);
        };
        let (leaf_tree_page, _) = BPlusTreeLeafPageCodec::decode(
            &leaf_page.read().unwrap().data,
            self.key_schema.clone(),
        )?;
        let result = leaf_tree_page.look_up(key);
        self.buffer_pool.unpin_page(leaf_page, false)?;
        Ok(result)
    }

    fn find_leaf_page(
        &mut self,
        key: &Tuple,
        context: &mut Context,
    ) -> BustubxResult<Option<Arc<RwLock<Page>>>> {
        if self.is_empty() {
            return Ok(None);
        }
        let mut curr_page = self.buffer_pool.fetch_page(self.root_page_id)?;
        let (mut curr_tree_page, _) =
            BPlusTreePageCodec::decode(&curr_page.read().unwrap().data, self.key_schema.clone())?;

        // 找到leaf page
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    context
                        .read_set
                        .push_back(curr_page.read().unwrap().page_id);
                    // 释放上一页
                    self.buffer_pool.unpin_page(curr_page, false)?;
                    // 查找下一页
                    let next_page_id = internal_page.look_up(key);
                    let next_page = self.buffer_pool.fetch_page(next_page_id)?;
                    let (next_tree_page, _) = BPlusTreePageCodec::decode(
                        &next_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )?;
                    curr_page = next_page;
                    curr_tree_page = next_tree_page;
                }
                BPlusTreePage::Leaf(_leaf_page) => {
                    self.buffer_pool.unpin_page(curr_page.clone(), false)?;
                    return Ok(Some(curr_page));
                }
            }
        }
    }

    // 分裂page
    fn split(&mut self, tree_page: &mut BPlusTreePage) -> BustubxResult<InternalKV> {
        let new_page = self.buffer_pool.new_page()?;
        let new_page_id = new_page.read().unwrap().page_id;

        match tree_page {
            BPlusTreePage::Leaf(leaf_page) => {
                // 拆分kv对
                let mut new_leaf_page =
                    BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
                new_leaf_page
                    .batch_insert(leaf_page.split_off(leaf_page.header.current_size as usize / 2));

                // 更新next page id
                new_leaf_page.header.next_page_id = leaf_page.header.next_page_id;
                leaf_page.header.next_page_id = new_page.read().unwrap().page_id;

                new_page.write().unwrap().data =
                    page_bytes_to_array(&BPlusTreeLeafPageCodec::encode(&new_leaf_page));
                self.buffer_pool.unpin_page_id(new_page_id, true)?;

                Ok((new_leaf_page.key_at(0).clone(), new_page_id))
            }
            BPlusTreePage::Internal(internal_page) => {
                // 拆分kv对
                let mut new_internal_page =
                    BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);
                new_internal_page.batch_insert(
                    internal_page.split_off(internal_page.header.current_size as usize / 2),
                );

                new_page.write().unwrap().data =
                    page_bytes_to_array(&BPlusTreeInternalPageCodec::encode(&new_internal_page));
                self.buffer_pool.unpin_page_id(new_page_id, true)?;

                let min_leafkv = self.find_subtree_min_leafkv(new_page_id)?;
                Ok((min_leafkv.0, new_page_id))
            }
        }
    }

    fn borrow(&mut self, _page: &mut BPlusTreePage, _context: &mut Context) {
        unimplemented!()
    }

    fn find_sibling_pages(
        &mut self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> BustubxResult<(Option<PageId>, Option<PageId>)> {
        let parent_page = self.buffer_pool.fetch_page(parent_page_id)?;
        let (parent_page, _) = BPlusTreeInternalPageCodec::decode(
            &parent_page.read().unwrap().data,
            self.key_schema.clone(),
        )?;
        self.buffer_pool.unpin_page_id(parent_page_id, false)?;
        Ok(parent_page.sibling_page_ids(child_page_id))
    }

    fn merge(&mut self, _page: &BPlusTreePage, _context: &mut Context) {
        unimplemented!()
    }

    // 查找子树最小的leafKV
    fn find_subtree_min_leafkv(&mut self, page_id: PageId) -> BustubxResult<LeafKV> {
        self.find_subtree_leafkv(page_id, true)
    }

    // 查找子树最大的leafKV
    fn find_subtree_max_leafkv(&mut self, page_id: PageId) -> BustubxResult<LeafKV> {
        self.find_subtree_leafkv(page_id, false)
    }

    fn find_subtree_leafkv(&mut self, page_id: PageId, min_or_max: bool) -> BustubxResult<LeafKV> {
        let curr_page = self.buffer_pool.fetch_page(page_id)?;
        let (mut curr_tree_page, _) =
            BPlusTreePageCodec::decode(&curr_page.read().unwrap().data, self.key_schema.clone())?;
        self.buffer_pool.unpin_page(curr_page.clone(), false)?;
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    let index = if min_or_max {
                        0
                    } else {
                        internal_page.header.current_size as usize - 1
                    };
                    let next_page_id = internal_page.value_at(index);
                    let next_page = self.buffer_pool.fetch_page(next_page_id)?;
                    curr_tree_page = BPlusTreePageCodec::decode(
                        &next_page.read().unwrap().data,
                        self.key_schema.clone(),
                    )?
                    .0;
                    self.buffer_pool.unpin_page(next_page, false)?;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    let index = if min_or_max {
                        0
                    } else {
                        leaf_page.header.current_size as usize - 1
                    };
                    return Ok(leaf_page.kv_at(index).clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::common::util::pretty_format_index_tree;
    use crate::{
        buffer::BufferPoolManager,
        catalog::{Column, DataType, Schema},
        common::rid::Rid,
        storage::{DiskManager, Tuple},
    };

    use super::BPlusTreeIndex;

    #[test]
    pub fn test_index_insert() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, Arc::new(disk_manager)));
        let mut index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 2, 3);

        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![1i8.into(), 2i16.into()]),
                Rid::new(1, 1),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![2i8.into(), 4i16.into()]),
                Rid::new(2, 2),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![3i8.into(), 6i16.into()]),
                Rid::new(3, 3),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![4i8.into(), 8i16.into()]),
                Rid::new(4, 4),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![5i8.into(), 10i16.into()]),
                Rid::new(5, 5),
            )
            .unwrap();

        assert_eq!(pretty_format_index_tree(&index).unwrap(),
                   "B+ Tree Level No.1:
+-----------------------+
| page_id=10, size: 2/3 |
+-----------------------+
| +------------+------+ |
| | NULL, NULL | 3, 6 | |
| +------------+------+ |
| | 6          | 9    | |
| +------------+------+ |
+-----------------------+
B+ Tree Level No.2:
+-----------------------+----------------------+
| page_id=6, size: 2/3  | page_id=9, size: 2/3 |
+-----------------------+----------------------+
| +------------+------+ | +------+------+      |
| | NULL, NULL | 2, 4 | | | 3, 6 | 4, 8 |      |
| +------------+------+ | +------+------+      |
| | 4          | 5    | | | 7    | 8    |      |
| +------------+------+ | +------+------+      |
+-----------------------+----------------------+
B+ Tree Level No.3:
+--------------------------------------+--------------------------------------+--------------------------------------+--------------------------------------+
| page_id=4, size: 1/2, next_page_id=5 | page_id=5, size: 1/2, next_page_id=7 | page_id=7, size: 1/2, next_page_id=8 | page_id=8, size: 2/2, next_page_id=0 |
+--------------------------------------+--------------------------------------+--------------------------------------+--------------------------------------+
| +------+                             | +------+                             | +------+                             | +------+-------+                     |
| | 1, 2 |                             | | 2, 4 |                             | | 3, 6 |                             | | 4, 8 | 5, 10 |                     |
| +------+                             | +------+                             | +------+                             | +------+-------+                     |
| | 1-1  |                             | | 2-2  |                             | | 3-3  |                             | | 4-4  | 5-5   |                     |
| +------+                             | +------+                             | +------+                             | +------+-------+                     |
+--------------------------------------+--------------------------------------+--------------------------------------+--------------------------------------+
"
        );
    }

    #[test]
    // TODO remove page id number
    pub fn test_index_delete() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, Arc::new(disk_manager)));
        let mut index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 4, 5);

        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
                Rid::new(1, 1),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
                Rid::new(2, 2),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
                Rid::new(3, 3),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
                Rid::new(4, 4),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
                Rid::new(5, 5),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]),
                Rid::new(6, 6),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![7i8.into(), 7i16.into()]),
                Rid::new(7, 7),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]),
                Rid::new(8, 8),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]),
                Rid::new(9, 9),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![10i8.into(), 10i16.into()]),
                Rid::new(10, 10),
            )
            .unwrap();
        assert_eq!(index.root_page_id, 6);

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![1i8.into(), 1i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![1i8.into(), 1i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![3i8.into(), 3i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![3i8.into(), 3i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![5i8.into(), 5i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![5i8.into(), 5i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![7i8.into(), 7i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![7i8.into(), 7i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![9i8.into(), 9i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![9i8.into(), 9i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![10i8.into(), 10i16.into()],
        ));
        assert_eq!(index.root_page_id, 6);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![10i8.into(), 10i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![8i8.into(), 8i16.into()],
        ));
        assert_eq!(index.root_page_id, 4);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![8i8.into(), 8i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![6i8.into(), 6i16.into()],
        ));
        assert_eq!(index.root_page_id, 4);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![6i8.into(), 6i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![4i8.into(), 4i16.into()],
        ));
        assert_eq!(index.root_page_id, 4);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![4i8.into(), 4i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![2i8.into(), 2i16.into()],
        ));
        assert_eq!(index.root_page_id, 4);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![2i8.into(), 2i16.into()]
                ))
                .unwrap(),
            None
        );

        index.delete(&Tuple::new(
            key_schema.clone(),
            vec![2i8.into(), 2i16.into()],
        ));
        assert_eq!(index.root_page_id, 4);
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![2i8.into(), 2i16.into()]
                ))
                .unwrap(),
            None
        );
        println!("{}", pretty_format_index_tree(&index).unwrap());
    }
}
