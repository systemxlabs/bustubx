use crate::catalog::schema::Schema;

#[derive(Debug, Clone)]
pub struct LogicalCreateIndexOperator {
    pub index_name: String,
    pub table_name: String,
    pub table_schema: Schema,
    pub key_attrs: Vec<u32>,
}
impl LogicalCreateIndexOperator {
    pub fn new(
        index_name: String,
        table_name: String,
        table_schema: Schema,
        key_attrs: Vec<u32>,
    ) -> Self {
        Self {
            index_name,
            table_name,
            table_schema,
            key_attrs,
        }
    }
}
