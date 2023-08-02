#[derive(Debug, Clone)]
pub struct InsertOperator {
    pub table_name: String,
}
impl InsertOperator {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}
