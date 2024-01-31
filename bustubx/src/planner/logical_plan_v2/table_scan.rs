use crate::catalog::ColumnRef;
use crate::common::table_ref::TableReference;
use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct TableScan {
    pub table_name: TableReference,
    pub columns: Vec<ColumnRef>,
    // TODO project push down
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}
