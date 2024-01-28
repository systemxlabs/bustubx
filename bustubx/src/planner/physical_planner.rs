use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;

use super::physical_plan::{build_plan, PhysicalPlan};

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_physical_plan(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        let logical_plan = Arc::new(logical_plan);
        build_plan(logical_plan.clone())
    }
}
