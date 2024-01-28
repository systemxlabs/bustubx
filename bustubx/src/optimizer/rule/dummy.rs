use crate::optimizer::heuristic::{
    graph::{HepGraph, HepNodeId},
    pattern::{Pattern, PatternChildrenPredicate},
    rule::Rule,
};
use crate::planner::operator::LogicalOperator;

lazy_static::lazy_static! {
    pub static ref DUMMY_RULE_PATTERN: Pattern = Pattern {
        predicate: |operator| matches!(operator, LogicalOperator::Dummy),
        children: PatternChildrenPredicate::None,
    };
}

#[derive(Debug, Clone)]
pub struct DummyRule;
impl Rule for DummyRule {
    fn pattern(&self) -> &Pattern {
        &DUMMY_RULE_PATTERN
    }
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        println!("DummyRule applied");
        true
    }
}

mod tests {
    use crate::optimizer::heuristic::{batch::HepBatchStrategy, HepOptimizer};
    use crate::planner::logical_plan::LogicalPlan;
    use crate::planner::operator::LogicalOperator;

    #[test]
    fn test_dummy_rule() {
        let plan = LogicalPlan {
            operator: LogicalOperator::Dummy,
            children: vec![],
        };

        let mut optimizer = HepOptimizer::new(plan).batch(
            "dummy",
            HepBatchStrategy::once_topdown(),
            vec![Box::new(super::DummyRule)],
        );

        let best_plan = optimizer.find_best();
        assert!(matches!(best_plan.operator, LogicalOperator::Dummy));
    }
}
