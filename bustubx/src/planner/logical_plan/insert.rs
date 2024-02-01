use crate::catalog::SchemaRef;
use crate::common::table_ref::TableReference;
use crate::planner::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Insert {
    pub table: TableReference,
    pub columns: Vec<String>,
    pub input: Arc<LogicalPlan>,
}
