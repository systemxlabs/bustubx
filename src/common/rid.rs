use crate::storage::page::PageId;

// Record Identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl Rid {
    pub const INVALID_RID: Self = Self {
        page_id: std::u32::MAX,
        slot_num: std::u32::MAX,
    };

    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }

    pub fn from_bytes(raw: &[u8]) -> Self {
        let page_id = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        let slot_num = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
        Self { page_id, slot_num }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[0..4].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.slot_num.to_be_bytes());
        bytes
    }
}
