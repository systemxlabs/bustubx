use crate::catalog::{catalog::TableOid, column::Column, schema::Schema};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalScanOperator {
    pub table_oid: TableOid,
    pub columns: Vec<Column>,
}
