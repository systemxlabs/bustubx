use crate::common::rid::Rid;

#[derive(Debug, Clone)]
pub struct Tuple {
    pub rid: Rid,
    pub data: Vec<u8>,
}
impl Tuple {
    pub fn new(rid: Rid, data: Vec<u8>) -> Self {
        Self { rid, data }
    }
    pub fn from_bytes(raw: &[u8]) -> Self {
        let data = raw.to_vec();
        Self {
            rid: Rid::INVALID_RID,
            data,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}
