use crate::common::table_ref::TableReference;
use crate::planner::logical_plan_v2::OrderByExpr;

#[derive(derive_new::new, Debug, Clone)]
pub struct CreateIndex {
    pub index_name: String,
    pub table: TableReference,
    pub columns: Vec<OrderByExpr>,
}
