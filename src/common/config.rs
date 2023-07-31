use crate::storage::page::PageId;

// 数据页的大小（字节）
pub const TINYSQL_PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageId = std::u32::MAX;

pub type TransactionId = u32;
