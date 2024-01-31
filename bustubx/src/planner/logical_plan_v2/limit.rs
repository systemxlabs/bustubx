use crate::planner::logical_plan_v2::LogicalPlanV2;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: usize,
    pub input: Arc<LogicalPlanV2>,
}
