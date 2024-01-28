use crate::optimizer::heuristic::{
    graph::{HepGraph, HepNodeId},
    pattern::{Pattern, PatternChildrenPredicate},
    rule::Rule,
};
use crate::planner::operator::LogicalOperator;

lazy_static::lazy_static! {
    static ref LIMIT_PROJECT_TRANSPOSE_RULE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, LogicalOperator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, LogicalOperator::Project(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Push down `Limit` past a `Project`.
#[derive(Debug, Clone)]
pub struct LimitProjectTranspose;
impl Rule for LimitProjectTranspose {
    fn pattern(&self) -> &Pattern {
        &LIMIT_PROJECT_TRANSPOSE_RULE_PATTERN
    }
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        graph.swap_node(node_id, graph.children_at(node_id)[0]);
        true
    }
}

mod tests {
    use std::sync::Arc;

    use crate::planner::logical_plan::LogicalPlan;
    use crate::planner::operator::LogicalOperator;
    use crate::{
        catalog::Column,
        catalog::DataType,
        optimizer::heuristic::{batch::HepBatchStrategy, HepOptimizer},
        planner::expr::{column_ref::ColumnRef, Expr},
    };

    #[test]
    pub fn test_limit_project_transpose() {
        // TODO not manually build plan until subquery is supported
        let logical_plan = LogicalPlan {
            operator: LogicalOperator::new_scan_operator(
                1,
                vec![Arc::new(Column::new("a".to_string(), DataType::Int32))],
            ),
            children: vec![],
        };
        let logical_plan = LogicalPlan {
            operator: LogicalOperator::new_project_operator(vec![Expr::ColumnRef(ColumnRef {
                relation: None,
                col_name: "a".to_string(),
            })]),
            children: vec![Arc::new(logical_plan)],
        };
        let logical_plan = LogicalPlan {
            operator: LogicalOperator::new_limit_operator(Some(10), None),
            children: vec![Arc::new(logical_plan)],
        };
        let mut optimizer = HepOptimizer::new(logical_plan).batch(
            "limit_project_transpose",
            HepBatchStrategy::once_topdown(),
            vec![Box::new(super::LimitProjectTranspose)],
        );
        let optimized_plan = optimizer.find_best();

        assert!(matches!(
            optimized_plan.operator,
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            optimized_plan.children[0].operator,
            LogicalOperator::Limit(_)
        ));
        assert!(matches!(
            optimized_plan.children[0].children[0].operator,
            LogicalOperator::Scan(_)
        ));
    }
}
