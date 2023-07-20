use std::mem::size_of;

use super::{page::PageId, tuple::Tuple};
use crate::common::{config::TINYSQL_PAGE_SIZE, rid::Rid};

pub const INTERNAL_PAGE_HEADER_SIZE: usize = 1 + 4;
pub const INTERNAL_PAGE_SIZE: usize =
    (TINYSQL_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / size_of::<InternalKVPair>();

pub const LEAF_PAGE_HEADER_SIZE: usize = 1 + 4 + 4;
pub const LEAF_PAGE_SIZE: usize =
    (TINYSQL_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / size_of::<LeafKVPair>();

#[derive(Debug, Clone, Copy)]
pub enum BPlusTreePageType {
    LeafPage,
    InternalPage,
    InvalidPage,
}

pub type InternalKVPair = (Tuple, PageId);
pub type LeafKVPair = (Tuple, Rid);

pub struct BPlusTreeInternalPage {
    pub page_type: BPlusTreePageType,
    pub max_size: u32,
    pub array: Vec<InternalKVPair>,
}
impl BPlusTreeInternalPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            page_type: BPlusTreePageType::InternalPage,
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
}

pub struct BPlusTreeLeafPage {
    pub page_type: BPlusTreePageType,
    pub max_size: u32,
    pub next_page_id: PageId,
    pub array: Vec<LeafKVPair>,
}
impl BPlusTreeLeafPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            page_type: BPlusTreePageType::LeafPage,
            max_size,
            next_page_id: 0,
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
}

mod tests {
    use std::mem::size_of;

    use crate::storage::tree_page::BPlusTreePageType;

    #[test]
    pub fn test_page_type_size() {
        assert_eq!(1, size_of::<BPlusTreePageType>())
    }
}
