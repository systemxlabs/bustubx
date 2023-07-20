use crate::storage::page::PageId;

// Record Identifier
#[derive(Debug, Clone, Copy)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_num: u32,
}
impl Rid {
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }
}
