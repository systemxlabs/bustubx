use crate::error::BustubxResult;
use crate::optimizer::rule::PushDownLimit;
use crate::planner::logical_plan::LogicalPlan;
use std::sync::Arc;

/// `LogicalOptimizerRule` transforms one [`LogicalPlan`] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer can simply return it as is.
pub trait LogicalOptimizerRule {
    /// Try and rewrite `plan` to an optimized form, returning None if the plan cannot be
    /// optimized by this rule.
    fn try_optimize(&self, plan: &LogicalPlan) -> BustubxResult<Option<LogicalPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer
    ///
    /// If a rule use default None, it should traverse recursively plan inside itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

#[derive(Clone)]
pub struct LogicalOptimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn LogicalOptimizerRule + Send + Sync>>,
}

impl LogicalOptimizer {
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn LogicalOptimizerRule + Sync + Send>> =
            vec![Arc::new(PushDownLimit {})];

        Self { rules }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> BustubxResult<LogicalPlan> {
        todo!()
    }
}
