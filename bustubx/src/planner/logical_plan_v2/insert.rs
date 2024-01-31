use crate::catalog::SchemaRef;
use crate::common::table_ref::TableReference;
use crate::planner::logical_plan_v2::LogicalPlanV2;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Insert {
    pub table: TableReference,
    pub schema: SchemaRef,
    pub input: Arc<LogicalPlanV2>,
}
