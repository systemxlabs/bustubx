#[derive(Debug)]
pub struct Integer {
    pub value: i32,
}
impl Integer {
    pub fn new(value: i32) -> Self {
        Self { value }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            value: i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        }
    }
}
