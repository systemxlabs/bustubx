use crate::error::BustubxResult;
use crate::optimizer::LogicalOptimizerRule;
use crate::planner::logical_plan_v2::LogicalPlanV2;

pub struct PushDownLimit;

impl LogicalOptimizerRule for PushDownLimit {
    fn try_optimize(&self, plan: &LogicalPlanV2) -> BustubxResult<Option<LogicalPlanV2>> {
        todo!()
    }

    fn name(&self) -> &str {
        "PushDownLimit"
    }
}
