use crate::common::config::BUSTUBX_PAGE_SIZE;

pub type PageId = u32;
pub const INVALID_PAGE_ID: PageId = u32::MAX;

#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageId,
    pub data: [u8; BUSTUBX_PAGE_SIZE],
    // 被引用次数
    pub pin_count: u32,
    // 是否被写过
    pub is_dirty: bool,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            data: [0; BUSTUBX_PAGE_SIZE],
            pin_count: 0,
            is_dirty: false,
        }
    }
    pub fn destroy(&mut self) {
        self.page_id = 0;
        self.data = [0; BUSTUBX_PAGE_SIZE];
        self.pin_count = 0;
        self.is_dirty = false;
    }
}
