pub struct SmallInt {
    pub value: i16,
}
impl SmallInt {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            value: i16::from_be_bytes([bytes[0], bytes[1]]),
        }
    }
}
