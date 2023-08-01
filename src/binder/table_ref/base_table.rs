use crate::catalog::{catalog::TableOid, schema::Schema};

#[derive(Debug)]
pub struct BoundBaseTableRef {
    pub table: String,
    pub oid: TableOid,
    pub alias: Option<String>,
    pub schema: Schema,
}
