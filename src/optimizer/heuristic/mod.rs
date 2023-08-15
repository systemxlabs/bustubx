use crate::planner::logical_plan::LogicalPlan;

pub struct HepOptimizer {}
impl HepOptimizer {
    // output optimized logical plan
    pub fn find_best(&self, logical_plan: LogicalPlan) -> LogicalPlan {
        // TODO implementation
        logical_plan
    }
}
