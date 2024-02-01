use crate::catalog::SchemaRef;
use crate::common::table_ref::TableReference;
use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct TableScan {
    pub table_ref: TableReference,
    pub table_schema: SchemaRef,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}
