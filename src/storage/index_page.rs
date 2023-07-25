use std::mem::size_of;

use super::{page::PageId, tuple::Tuple};
use crate::{
    catalog::schema::Schema,
    common::{
        config::{INVALID_PAGE_ID, TINYSQL_PAGE_SIZE},
        rid::Rid,
    },
};

pub const INTERNAL_PAGE_HEADER_SIZE: usize = 4 + 4 + 4;
pub const LEAF_PAGE_HEADER_SIZE: usize = 4 + 4 + 4 + 4;

#[derive(Debug, Clone)]
pub enum BPlusTreePage {
    // B+树内部节点页
    Internal(BPlusTreeInternalPage),
    // B+树叶子节点页
    Leaf(BPlusTreeLeafPage),
}
impl BPlusTreePage {
    pub fn from_bytes(raw: &[u8; TINYSQL_PAGE_SIZE], key_schema: &Schema) -> Self {
        let page_type = BPlusTreePageType::from_bytes(&raw[0..4].try_into().unwrap());
        return match page_type {
            BPlusTreePageType::InternalPage => {
                Self::Internal(BPlusTreeInternalPage::from_bytes(raw, key_schema))
            }
            BPlusTreePageType::LeafPage => {
                Self::Leaf(BPlusTreeLeafPage::from_bytes(raw, key_schema))
            }
            BPlusTreePageType::InvalidPage => panic!("Invalid b+ tree page type"),
        };
    }
    pub fn to_bytes(&self) -> [u8; TINYSQL_PAGE_SIZE] {
        match self {
            Self::Internal(page) => page.to_bytes(),
            Self::Leaf(page) => page.to_bytes(),
        }
    }
    pub fn is_leaf(&self) -> bool {
        match self {
            Self::Internal(_) => false,
            Self::Leaf(_) => true,
        }
    }
    pub fn is_full(&self) -> bool {
        match self {
            Self::Internal(page) => page.is_full(),
            Self::Leaf(page) => page.is_full(),
        }
    }
    pub fn insert_internalkv(&mut self, internalkv: InternalKV, key_schema: &Schema) {
        match self {
            Self::Internal(page) => page.insert(internalkv.0, internalkv.1, key_schema),
            Self::Leaf(_) => panic!("Leaf page cannot insert InternalKV"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BPlusTreePageType {
    InvalidPage,
    LeafPage,
    InternalPage,
}
impl BPlusTreePageType {
    pub fn from_bytes(raw: &[u8; 4]) -> Self {
        match u32::from_be_bytes(*raw) {
            0 => Self::InvalidPage,
            1 => Self::LeafPage,
            2 => Self::InternalPage,
            _ => panic!("Invalid page type"),
        }
    }
    pub fn to_bytes(&self) -> [u8; 4] {
        match self {
            Self::InvalidPage => 0u32.to_be_bytes(),
            Self::LeafPage => 1u32.to_be_bytes(),
            Self::InternalPage => 2u32.to_be_bytes(),
        }
    }
}

pub type InternalKV = (Tuple, PageId);
pub type LeafKV = (Tuple, Rid);

/**
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 *
 * Header format (size in byte, 12 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 */
#[derive(Debug, Clone)]
pub struct BPlusTreeInternalPage {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    pub max_size: u32,
    // 第一个key为空，n个key对应n+1个value
    pub array: Vec<InternalKV>,
}
impl BPlusTreeInternalPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            page_type: BPlusTreePageType::InternalPage,
            current_size: 0,
            max_size,
            array: Vec::with_capacity(max_size as usize),
        }
    }
    pub fn size(&self) -> usize {
        self.array.len()
    }
    pub fn min_size(&self) -> usize {
        self.max_size as usize / 2
    }
    pub fn key_at(&self, index: usize) -> &Tuple {
        &self.array[index].0
    }
    pub fn value_at(&self, index: usize) -> PageId {
        self.array[index].1
    }
    pub fn insert(&mut self, key: Tuple, page_id: PageId, key_schema: &Schema) {
        self.array.push((key, page_id));
        self.current_size += 1;
        self.array.sort_by(|a, b| a.0.compare(&b.0, key_schema));
    }
    pub fn batch_insert(&mut self, kvs: Vec<InternalKV>, key_schema: &Schema) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.current_size += kvs_len as u32;
        self.array.sort_by(|a, b| a.0.compare(&b.0, key_schema));
    }
    pub fn is_full(&self) -> bool {
        self.current_size > self.max_size
    }
    pub fn split_off(&mut self) -> Vec<InternalKV> {
        let new_array = self.array.split_off(self.current_size as usize / 2);
        self.current_size -= new_array.len() as u32;
        return new_array;
    }

    // 查找key对应的page_id
    // TODO 增加测试用例
    pub fn look_up(&self, key: &Tuple, key_schema: &Schema) -> PageId {
        // 第一个key为空，所以从1开始
        let mut start = 1;
        let mut end = self.current_size - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = key.compare(&self.array[mid as usize].0, key_schema);
            if compare_res == std::cmp::Ordering::Equal {
                return self.array[mid as usize].1;
            } else if compare_res == std::cmp::Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        let compare_res = key.compare(&self.array[start as usize].0, key_schema);
        if compare_res == std::cmp::Ordering::Less {
            return self.array[start as usize - 1].1;
        } else {
            return self.array[start as usize].1;
        }
    }

    pub fn from_bytes(raw: &[u8; TINYSQL_PAGE_SIZE], key_schema: &Schema) -> Self {
        let page_type = BPlusTreePageType::from_bytes(&raw[0..4].try_into().unwrap());
        let current_size = u32::from_be_bytes(raw[4..8].try_into().unwrap());
        let max_size = u32::from_be_bytes(raw[8..12].try_into().unwrap());
        let mut array = Vec::with_capacity(max_size as usize);
        let key_size = key_schema.fixed_len();
        let value_size = size_of::<PageId>();
        let kv_size = key_size + value_size;
        for i in 0..current_size {
            let start = 12 + i as usize * kv_size;
            let end = 12 + (i + 1) as usize * kv_size;
            let key = Tuple::from_bytes(&raw[start..start + key_size]);
            let page_id = u32::from_be_bytes(raw[start + key_size..end].try_into().unwrap());
            array.push((key, page_id));
        }
        Self {
            page_type,
            current_size,
            max_size,
            array,
        }
    }

    pub fn to_bytes(&self) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        buf[0..4].copy_from_slice(&self.page_type.to_bytes());
        buf[4..8].copy_from_slice(&self.current_size.to_be_bytes());
        buf[8..12].copy_from_slice(&self.max_size.to_be_bytes());
        let key_size = self.array[0].0.data.len();
        let value_size = size_of::<PageId>();
        let kv_size = key_size + value_size;
        for i in 0..self.current_size {
            let start = 12 + i as usize * kv_size;
            let end = 12 + (i + 1) as usize * kv_size;
            buf[start..start + key_size].copy_from_slice(&self.array[i as usize].0.to_bytes());
            buf[start + key_size..end].copy_from_slice(&self.array[i as usize].1.to_be_bytes());
        }
        buf
    }
}

/**
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4)
 *  ---------------------------------------------------------------------
 */
#[derive(Debug, Clone)]
pub struct BPlusTreeLeafPage {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    pub max_size: u32,
    pub next_page_id: PageId,
    pub array: Vec<LeafKV>,
}
impl BPlusTreeLeafPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            page_type: BPlusTreePageType::LeafPage,
            current_size: 0,
            max_size,
            next_page_id: INVALID_PAGE_ID,
            array: Vec::with_capacity(max_size as usize),
        }
    }
    pub fn from_bytes(raw: &[u8; TINYSQL_PAGE_SIZE], key_schema: &Schema) -> Self {
        let page_type = BPlusTreePageType::from_bytes(&raw[0..4].try_into().unwrap());
        let current_size = u32::from_be_bytes(raw[4..8].try_into().unwrap());
        let max_size = u32::from_be_bytes(raw[8..12].try_into().unwrap());
        let next_page_id = u32::from_be_bytes(raw[12..16].try_into().unwrap());
        let mut array = Vec::with_capacity(max_size as usize);
        let key_size = key_schema.fixed_len();
        let value_size = size_of::<Rid>();
        let kv_size = key_size + value_size;
        for i in 0..current_size {
            let start = 16 + i as usize * kv_size;
            let end = 16 + (i + 1) as usize * kv_size;
            let key = Tuple::from_bytes(&raw[start..start + key_size]);
            let rid = Rid::from_bytes(raw[start + key_size..end].try_into().unwrap());
            array.push((key, rid));
        }
        Self {
            page_type,
            current_size,
            max_size,
            next_page_id,
            array,
        }
    }

    pub fn to_bytes(&self) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        buf[0..4].copy_from_slice(&self.page_type.to_bytes());
        buf[4..8].copy_from_slice(&self.current_size.to_be_bytes());
        buf[8..12].copy_from_slice(&self.max_size.to_be_bytes());
        buf[12..16].copy_from_slice(&self.next_page_id.to_be_bytes());
        let key_size = self.array[0].0.data.len();
        let value_size = size_of::<Rid>();
        let kv_size = key_size + value_size;
        for i in 0..self.current_size {
            let start = 16 + i as usize * kv_size;
            let end = 16 + (i + 1) as usize * kv_size;
            buf[start..start + key_size].copy_from_slice(&self.array[i as usize].0.to_bytes());
            buf[start + key_size..end].copy_from_slice(&self.array[i as usize].1.to_bytes());
        }
        buf
    }

    pub fn size(&self) -> usize {
        self.current_size as usize
    }
    pub fn min_size(&self) -> usize {
        self.max_size as usize / 2
    }
    pub fn key_at(&self, index: usize) -> &Tuple {
        &self.array[index].0
    }
    pub fn kv_at(&self, index: usize) -> &LeafKV {
        &self.array[index]
    }
    pub fn is_full(&self) -> bool {
        self.current_size > self.max_size
    }
    pub fn insert(&mut self, key: Tuple, rid: Rid, key_schema: &Schema) {
        self.array.push((key, rid));
        self.current_size += 1;
        self.array.sort_by(|a, b| a.0.compare(&b.0, key_schema));
    }
    pub fn batch_insert(&mut self, kvs: Vec<LeafKV>, key_schema: &Schema) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.current_size += kvs_len as u32;
        self.array.sort_by(|a, b| a.0.compare(&b.0, key_schema));
    }
    pub fn split_off(&mut self) -> Vec<LeafKV> {
        let new_array = self.array.split_off(self.current_size as usize / 2);
        self.current_size -= new_array.len() as u32;
        return new_array;
    }

    // 查找key对应的rid
    // TODO 增加测试用例
    pub fn look_up(&self, key: &Tuple, key_schema: &Schema) -> Option<Rid> {
        let mut start: i32 = 0;
        let mut end: i32 = self.current_size as i32 - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = key.compare(&self.array[mid as usize].0, key_schema);
            if compare_res == std::cmp::Ordering::Equal {
                return Some(self.array[mid as usize].1);
            } else if compare_res == std::cmp::Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        if key.compare(&self.array[start as usize].0, key_schema) == std::cmp::Ordering::Equal {
            return Some(self.array[start as usize].1);
        }
        None
    }
}

mod tests {
    use std::mem::size_of;

    use crate::{
        catalog::{
            column::{Column, DataType},
            schema::Schema,
        },
        common::rid::Rid,
        storage::{
            index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePageType, InternalKV},
            tuple::Tuple,
        },
    };

    #[test]
    pub fn test_internal_page_from_to_bytes() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("a".to_string(), DataType::SmallInt, 0),
        ]);
        let mut ori_page = BPlusTreeInternalPage::new(5);
        ori_page.insert(
            Tuple::new_with_rid(Rid::new(1, 1), vec![1, 1, 1]),
            1,
            &key_schema,
        );
        ori_page.insert(
            Tuple::new_with_rid(Rid::new(2, 2), vec![2, 2, 2]),
            2,
            &key_schema,
        );
        assert_eq!(ori_page.current_size, 2);

        let bytes = ori_page.to_bytes();

        let new_page = BPlusTreeInternalPage::from_bytes(&bytes, &key_schema);
        assert_eq!(new_page.page_type, BPlusTreePageType::InternalPage);
        assert_eq!(new_page.current_size, 2);
        assert_eq!(new_page.max_size, 5);
        assert_eq!(new_page.array[0].0.data, vec![1, 1, 1]);
        assert_eq!(new_page.array[0].1, 1);
        assert_eq!(new_page.array[1].0.data, vec![2, 2, 2]);
        assert_eq!(new_page.array[1].1, 2);
    }

    #[test]
    pub fn test_leaf_page_from_to_bytes() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("a".to_string(), DataType::SmallInt, 0),
        ]);
        let mut ori_page = BPlusTreeLeafPage::new(5);
        ori_page.insert(Tuple::new(vec![1, 1, 1]), Rid::new(1, 1), &key_schema);
        ori_page.insert(Tuple::new(vec![2, 2, 2]), Rid::new(2, 2), &key_schema);
        assert_eq!(ori_page.current_size, 2);

        let bytes = ori_page.to_bytes();

        let new_page = BPlusTreeLeafPage::from_bytes(&bytes, &key_schema);
        assert_eq!(new_page.page_type, BPlusTreePageType::LeafPage);
        assert_eq!(new_page.current_size, 2);
        assert_eq!(new_page.max_size, 5);
        assert_eq!(new_page.array[0].0.data, vec![1, 1, 1]);
        assert_eq!(new_page.array[0].1, Rid::new(1, 1));
        assert_eq!(new_page.array[1].0.data, vec![2, 2, 2]);
        assert_eq!(new_page.array[1].1, Rid::new(2, 2));
    }

    #[test]
    pub fn test_internal_page_insert() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
        ]);
        let mut internal_page = BPlusTreeInternalPage::new(3);
        internal_page.insert(Tuple::new(vec![2, 2, 2]), 2, &key_schema);
        internal_page.insert(Tuple::empty(key_schema.fixed_len()), 0, &key_schema);
        internal_page.insert(Tuple::new(vec![1, 1, 1]), 1, &key_schema);
        assert_eq!(internal_page.current_size, 3);
        assert_eq!(internal_page.array[0].0.data, vec![0, 0, 0]);
        assert_eq!(internal_page.array[0].1, 0);
        assert_eq!(internal_page.array[1].0.data, vec![1, 1, 1]);
        assert_eq!(internal_page.array[1].1, 1);
        assert_eq!(internal_page.array[2].0.data, vec![2, 2, 2]);
        assert_eq!(internal_page.array[2].1, 2);
    }

    #[test]
    pub fn test_leaf_page_insert() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
        ]);
        let mut leaf_page = BPlusTreeLeafPage::new(3);
        leaf_page.insert(Tuple::new(vec![2, 2, 2]), Rid::new(2, 2), &key_schema);
        leaf_page.insert(Tuple::new(vec![1, 1, 1]), Rid::new(1, 1), &key_schema);
        leaf_page.insert(Tuple::new(vec![3, 3, 3]), Rid::new(3, 3), &key_schema);
        assert_eq!(leaf_page.current_size, 3);
        assert_eq!(leaf_page.array[0].0.data, vec![1, 1, 1]);
        assert_eq!(leaf_page.array[0].1, Rid::new(1, 1));
        assert_eq!(leaf_page.array[1].0.data, vec![2, 2, 2]);
        assert_eq!(leaf_page.array[1].1, Rid::new(2, 2));
        assert_eq!(leaf_page.array[2].0.data, vec![3, 3, 3]);
        assert_eq!(leaf_page.array[2].1, Rid::new(3, 3));
    }

    #[test]
    pub fn test_internal_page_look_up() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
        ]);
        let mut internal_page = BPlusTreeInternalPage::new(5);
        internal_page.insert(Tuple::new(vec![2, 2, 2]), 2, &key_schema);
        internal_page.insert(Tuple::new(vec![1, 1, 1]), 1, &key_schema);
        internal_page.insert(Tuple::new(vec![3, 3, 3]), 3, &key_schema);
        internal_page.insert(Tuple::empty(key_schema.fixed_len()), 0, &key_schema);
        internal_page.insert(Tuple::new(vec![4, 4, 4]), 4, &key_schema);

        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![0, 0, 0]), &key_schema),
            0
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![3, 3, 3]), &key_schema),
            3
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![5, 5, 5]), &key_schema),
            4
        );

        let mut internal_page = BPlusTreeInternalPage::new(2);
        internal_page.insert(Tuple::empty(key_schema.fixed_len()), 0, &key_schema);
        internal_page.insert(Tuple::new(vec![1, 1, 1]), 1, &key_schema);

        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![0, 0, 0]), &key_schema),
            0
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![1, 1, 1]), &key_schema),
            1
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(vec![2, 2, 2]), &key_schema),
            1
        );
    }

    #[test]
    pub fn test_leaf_page_look_up() {
        let key_schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
        ]);
        let mut leaf_page = BPlusTreeLeafPage::new(5);
        leaf_page.insert(Tuple::new(vec![2, 2, 2]), Rid::new(2, 2), &key_schema);
        leaf_page.insert(Tuple::new(vec![1, 1, 1]), Rid::new(1, 1), &key_schema);
        leaf_page.insert(Tuple::new(vec![3, 3, 3]), Rid::new(3, 3), &key_schema);
        leaf_page.insert(Tuple::new(vec![5, 5, 5]), Rid::new(5, 5), &key_schema);
        leaf_page.insert(Tuple::new(vec![4, 4, 4]), Rid::new(4, 4), &key_schema);
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![0, 0, 0]), &key_schema),
            None
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![2, 2, 2]), &key_schema),
            Some(Rid::new(2, 2))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![3, 3, 3]), &key_schema),
            Some(Rid::new(3, 3))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![6, 6, 6]), &key_schema),
            None
        );

        let mut leaf_page = BPlusTreeLeafPage::new(2);
        leaf_page.insert(Tuple::new(vec![2, 2, 2]), Rid::new(2, 2), &key_schema);
        leaf_page.insert(Tuple::new(vec![1, 1, 1]), Rid::new(1, 1), &key_schema);
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![0, 0, 0]), &key_schema),
            None
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![1, 1, 1]), &key_schema),
            Some(Rid::new(1, 1))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![2, 2, 2]), &key_schema),
            Some(Rid::new(2, 2))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(vec![3, 3, 3]), &key_schema),
            None
        );
    }
}
