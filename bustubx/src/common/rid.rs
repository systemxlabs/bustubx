use crate::buffer::PageId;

// TODO should move to table page?
// Record Identifier
#[derive(derive_new::new, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl Rid {
    pub const INVALID_RID: Self = Self {
        page_id: u32::MAX,
        slot_num: u32::MAX,
    };
}
