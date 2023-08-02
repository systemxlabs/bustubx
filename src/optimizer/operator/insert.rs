#[derive(Debug)]
pub struct PhysicalInsertOperator {
    pub table_name: String,
}
impl PhysicalInsertOperator {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}
