use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::planner::logical_plan_v2::LogicalPlanV2;
use crate::planner::table_ref::join::JoinType;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlanV2>,
    /// Right input
    pub right: Arc<LogicalPlanV2>,
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub schema: SchemaRef,
}
