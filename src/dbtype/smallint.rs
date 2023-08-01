#[derive(Debug)]
pub struct SmallInt {
    pub value: i16,
}
impl SmallInt {
    pub fn new(value: i16) -> Self {
        Self { value }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            value: i16::from_be_bytes([bytes[0], bytes[1]]),
        }
    }
}
