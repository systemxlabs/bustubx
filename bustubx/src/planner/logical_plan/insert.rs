use crate::catalog::SchemaRef;
use crate::common::TableReference;
use crate::planner::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Insert {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    pub input: Arc<LogicalPlan>,
}
