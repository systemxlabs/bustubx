use crate::catalog::{catalog::TableOid, schema::Schema};

#[derive(Debug, Clone)]
pub struct BoundBaseTableRef {
    pub table: String,
    pub oid: TableOid,
    pub alias: Option<String>,
    pub schema: Schema,
}
impl BoundBaseTableRef {
    pub fn column_names(&self) -> Vec<String> {
        self.schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }
}
