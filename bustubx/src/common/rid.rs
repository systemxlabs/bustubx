use crate::buffer::{PageId, INVALID_PAGE_ID};

pub const INVALID_RID: Rid = Rid {
    page_id: INVALID_PAGE_ID,
    slot_num: 0,
};

// TODO should move to table page?
// Record Identifier
#[derive(derive_new::new, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_num: u32,
}
