#[derive(Debug, Clone, Copy)]
pub struct Boolean {
    pub value: bool,
}
impl Boolean {
    pub fn new(value: bool) -> Self {
        Self { value }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            value: bytes[0] != 0,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.value {
            vec![1]
        } else {
            vec![0]
        }
    }
}
