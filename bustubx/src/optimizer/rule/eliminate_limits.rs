use crate::optimizer::heuristic::{
    graph::{HepGraph, HepNodeId},
    pattern::{Pattern, PatternChildrenPredicate},
    rule::Rule,
};
use crate::planner::operator::{limit::LogicalLimitOperator, LogicalOperator};

lazy_static::lazy_static! {
    static ref ELIMINATE_LIMITS_RULE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, LogicalOperator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, LogicalOperator::Limit(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Combines two adjacent Limit operators into one, merging the expressions into one single expr.
#[derive(Debug, Clone)]
pub struct EliminateLimits;
impl Rule for EliminateLimits {
    fn pattern(&self) -> &Pattern {
        &ELIMINATE_LIMITS_RULE_PATTERN
    }
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        if let Some(LogicalOperator::Limit(op)) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Some(LogicalOperator::Limit(child_op)) = graph.operator(child_id) {
                let new_limit_op = LogicalLimitOperator {
                    offset: Some(op.offset.unwrap_or(0) + child_op.offset.unwrap_or(0)),
                    limit: std::cmp::min(op.limit, child_op.limit),
                };

                graph.remove_node(child_id, false);
                graph.replace_node(node_id, LogicalOperator::Limit(new_limit_op));
                return true;
            }
        }
        return false;
    }
}
