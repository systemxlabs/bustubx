use crate::common::config::TINYSQL_PAGE_SIZE;

pub type PageId = u32;

#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageId,
    pub data: [u8; TINYSQL_PAGE_SIZE],
    // 被引用次数
    pub pin_count: u32,
    // 是否被写过
    pub is_dirty: bool,
}
impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            data: [0; TINYSQL_PAGE_SIZE],
            pin_count: 0,
            is_dirty: false,
        }
    }
}
