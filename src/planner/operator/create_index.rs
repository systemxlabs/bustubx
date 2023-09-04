use crate::catalog::schema::Schema;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalCreateIndexOperator {
    pub index_name: String,
    pub table_name: String,
    pub table_schema: Schema,
    pub key_attrs: Vec<u32>,
}
