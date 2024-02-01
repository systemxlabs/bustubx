use crate::catalog::SchemaRef;
use crate::common::table_ref::TableReference;
use crate::planner::logical_plan::OrderByExpr;

#[derive(derive_new::new, Debug, Clone)]
pub struct CreateIndex {
    pub index_name: String,
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub columns: Vec<OrderByExpr>,
}
