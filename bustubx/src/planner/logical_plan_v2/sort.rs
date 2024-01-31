use crate::expression::Expr;
use crate::planner::logical_plan_v2::LogicalPlanV2;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Sort {
    pub expr: Vec<OrderByExpr>,
    pub input: Arc<LogicalPlanV2>,
    pub limit: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OrderByExpr {
    /// The expression to sort on
    pub expr: Box<Expr>,
    /// The direction of the sort
    pub asc: bool,
    /// Whether to put Nulls before all other data values
    pub nulls_first: bool,
}
