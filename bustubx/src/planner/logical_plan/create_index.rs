use crate::catalog::SchemaRef;
use crate::common::TableReference;
use crate::planner::logical_plan::OrderByExpr;

#[derive(derive_new::new, Debug, Clone)]
pub struct CreateIndex {
    pub index_name: String,
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub columns: Vec<OrderByExpr>,
}

impl std::fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateIndex: {}", self.index_name)
    }
}
