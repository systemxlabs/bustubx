use tracing::span;

use crate::planner::logical_plan::LogicalPlan;

use self::{physical_optimizer::PhysicalOptimizer, physical_plan::PhysicalPlan};

pub mod physical_optimizer;
pub mod physical_plan;

pub struct Optimizer {
    physical_optimizer: PhysicalOptimizer,
}
impl Optimizer {
    pub fn new() -> Self {
        Self {
            physical_optimizer: PhysicalOptimizer {},
        }
    }

    pub fn find_best(&mut self, logical_plan: LogicalPlan) -> PhysicalPlan {
        let _find_best_span = span!(tracing::Level::INFO, "optimizer.find_best").entered();
        // TODO optimize logical plan

        // optimize physical plan
        self.physical_optimizer.find_best(logical_plan)
    }
}
