use std::sync::Arc;

use crate::planner::logical_plan::{LogicalPlan, self};

pub mod batch;
pub mod rule;
pub mod pattern;
pub mod graph;
pub mod matcher;
pub mod opt_expr;

pub struct HepOptimizer {

}
impl HepOptimizer {
    // output the optimized logical plan
    pub fn find_best(&self, logical_plan: LogicalPlan) -> LogicalPlan {
        // TODO
        logical_plan
    }
}
impl Default for HepOptimizer {
    fn default() -> Self {
        Self{}
    }
}