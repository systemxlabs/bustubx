use crate::catalog::catalog::TableOid;
use crate::catalog::column::ColumnRef;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalScanOperator {
    pub table_oid: TableOid,
    pub columns: Vec<ColumnRef>,
}
