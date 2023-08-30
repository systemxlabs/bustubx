use crate::{
    optimizer::heuristic::{
        graph::{HepGraph, HepNodeId},
        pattern::{Pattern, PatternChildrenPredicate},
        rule::Rule,
    },
    planner::operator::LogicalOperator,
};

lazy_static::lazy_static! {
    static ref PUSH_LIMIT_INTO_SCAN_RULE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, LogicalOperator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, LogicalOperator::Scan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Push down `Limit` into `Scan`.
#[derive(Debug, Clone)]
pub struct PushLimitIntoScan;
impl Rule for PushLimitIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_INTO_SCAN_RULE_PATTERN
    }
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        // TODO nees scan operator to support limit
        unimplemented!()
    }
}
