use crate::planner::operator::LogicalOperator;
use crate::{
    optimizer::heuristic::{
        graph::{HepGraph, HepNodeId},
        pattern::{Pattern, PatternChildrenPredicate},
        rule::Rule,
    },
    planner::table_ref::join::JoinType,
};

lazy_static::lazy_static! {
    static ref PUSH_LIMIT_THROUGH_JOIN_RULE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, LogicalOperator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, LogicalOperator::Join(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Add extra limits below JOIN:
/// 1. For LEFT OUTER and RIGHT OUTER JOIN, we push limits to the left and right sides, respectively.
/// 2. For FULL OUTER, INNER and CROSS JOIN, we push limits to both the left and right sides if join condition is empty.
#[derive(Debug, Clone)]
pub struct PushLimitThroughJoin;
impl Rule for PushLimitThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_THROUGH_JOIN_RULE_PATTERN
    }
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        let child_id = graph.children_at(node_id)[0];
        let (join_type, condition) =
            if let Some(LogicalOperator::Join(op)) = graph.operator(child_id) {
                (Some(op.join_type), op.condition.clone())
            } else {
                (None, None)
            };

        if let Some(join_type) = join_type {
            let grandson_ids = match join_type {
                JoinType::LeftOuter => vec![graph.children_at(child_id)[0]],
                JoinType::RightOuter => vec![graph.children_at(child_id)[1]],
                JoinType::FullOuter | JoinType::CrossJoin | JoinType::Inner => {
                    if condition.is_none() {
                        vec![
                            graph.children_at(child_id)[0],
                            graph.children_at(child_id)[1],
                        ]
                    } else {
                        vec![]
                    }
                }
            };
            let limit_op = graph.remove_node(node_id, false).unwrap();

            for grandson_id in grandson_ids {
                graph.insert_node(child_id, Some(grandson_id), limit_op.clone());
            }
        }
        unimplemented!()
    }
}
