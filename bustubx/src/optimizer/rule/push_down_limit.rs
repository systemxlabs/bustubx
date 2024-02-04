use crate::error::BustubxResult;
use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::planner::logical_plan::LogicalPlan;

pub struct PushDownLimit;

impl LogicalOptimizerRule for PushDownLimit {
    fn try_optimize(&self, plan: &LogicalPlan) -> BustubxResult<Option<LogicalPlan>> {
        todo!()
    }

    fn name(&self) -> &str {
        "PushDownLimit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}
