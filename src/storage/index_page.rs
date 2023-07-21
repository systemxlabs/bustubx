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

#[derive(Debug)]
pub enum BPlusTreePage {
    Internal(BPlusTreeInternalPage),
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
#[derive(Debug)]
pub struct BPlusTreeInternalPage {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    pub max_size: u32,
    array: Vec<InternalKV>,
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
    pub fn insert(&mut self, key: Tuple, page_id: PageId) {
        assert!(self.current_size < self.max_size, "Internal page is full");
        self.array.push((key, page_id));
        self.current_size += 1;
        // TODO sort
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
#[derive(Debug)]
pub struct BPlusTreeLeafPage {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    pub max_size: u32,
    pub next_page_id: PageId,
    array: Vec<LeafKV>,
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
    pub fn insert(&mut self, key: Tuple, rid: Rid) {
        assert!(self.current_size < self.max_size, "Leaf page is full");
        self.array.push((key, rid));
        self.current_size += 1;
        // TODO sort
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
        ori_page.insert(Tuple::new(Rid::new(1, 1), vec![1, 1, 1]), 1);
        ori_page.insert(Tuple::new(Rid::new(2, 2), vec![2, 2, 2]), 2);
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
        ori_page.insert(Tuple::new(Rid::INVALID_RID, vec![1, 1, 1]), Rid::new(1, 1));
        ori_page.insert(Tuple::new(Rid::INVALID_RID, vec![2, 2, 2]), Rid::new(2, 2));
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
}
