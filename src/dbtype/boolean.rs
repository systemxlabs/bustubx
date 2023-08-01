#[derive(Debug)]
pub struct Boolean {
    pub value: bool,
}
impl Boolean {
    pub fn new(value: bool) -> Self {
        Self { value }
    }
}
