use crate::common::config::TINYSQL_PAGE_SIZE;

pub type PageId = u32;

#[derive(Debug)]
pub struct Page {
    pub page_id: PageId,
    pub data: [u8; TINYSQL_PAGE_SIZE],
    // 被引用次数
    pub pin_count: u32,
    // 是否被写过
    pub is_dirty: bool,
}