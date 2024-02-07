use crate::buffer::PageId;

// Record Identifier
#[derive(derive_new::new, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl Rid {
    pub const INVALID_RID: Self = Self {
        page_id: std::u32::MAX,
        slot_num: std::u32::MAX,
    };
}
