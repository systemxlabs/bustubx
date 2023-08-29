use std::sync::Arc;

use crate::planner::{
    logical_plan::{self, LogicalPlan},
    operator::LogicalOperator,
};

use super::physical_plan_v2::{build_plan_v2, PhysicalPlanV2};

pub struct PhysicalOptimizer {}
impl PhysicalOptimizer {
    // output optimized physical plan
    pub fn find_best_v2(&self, logical_plan: LogicalPlan) -> PhysicalPlanV2 {
        // TODO optimization
        let logical_plan = Arc::new(logical_plan);
        build_plan_v2(logical_plan.clone())
    }
}
