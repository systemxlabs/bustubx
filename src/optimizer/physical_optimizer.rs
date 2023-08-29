use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;

use super::physical_plan::{build_plan, PhysicalPlan};

pub struct PhysicalOptimizer {}
impl PhysicalOptimizer {
    // output optimized physical plan
    pub fn find_best(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        // TODO optimization
        let logical_plan = Arc::new(logical_plan);
        build_plan(logical_plan.clone())
    }
}
