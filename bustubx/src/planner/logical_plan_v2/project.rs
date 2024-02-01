use crate::expression::Expr;
use crate::planner::logical_plan_v2::LogicalPlanV2;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Project {
    pub exprs: Vec<Expr>,
    pub input: Arc<LogicalPlanV2>,
}
