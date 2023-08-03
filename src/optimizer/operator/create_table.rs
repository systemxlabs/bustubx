use crate::catalog::schema::Schema;

#[derive(Debug)]
pub struct PhysicalCreateTableOperator {
    pub table_name: String,
    pub schema: Schema,
}
impl PhysicalCreateTableOperator {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }
    pub fn output_schema(&self) -> Schema {
        self.schema.clone()
    }
}
