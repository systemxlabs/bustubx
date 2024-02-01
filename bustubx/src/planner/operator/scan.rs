use crate::catalog::ColumnRef;
use crate::catalog::TableOid;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalScanOperator {
    pub table_name: String,
    pub columns: Vec<ColumnRef>,
}
