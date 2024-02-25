use derive_with::With;
use std::sync::atomic::AtomicU32;

pub type PageId = u32;
pub type AtomicPageId = AtomicU32;

pub const INVALID_PAGE_ID: PageId = 0;
pub const BUSTUBX_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, With)]
pub struct Page {
    pub page_id: PageId,
    data: [u8; BUSTUBX_PAGE_SIZE],
    // 被引用次数
    pub pin_count: u32,
    // 是否被写过
    pub is_dirty: bool,
}

impl Page {
    pub fn empty() -> Self {
        Self::new(INVALID_PAGE_ID)
    }
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

    pub fn set_data(&mut self, data: [u8; BUSTUBX_PAGE_SIZE]) {
        self.data = data;
        self.is_dirty = true;
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn replace(&mut self, other: Page) {
        self.page_id = other.page_id;
        self.data = other.data;
        self.pin_count = other.pin_count;
        self.is_dirty = other.is_dirty;
    }
}
