use crate::optimizer::heuristic::pattern::PatternChildrenPredicate;

use super::{
    batch::HepMatchOrder,
    graph::{HepGraph, HepNodeId},
    pattern::Pattern,
};

pub struct HepMatcher<'a, 'b> {
    pub pattern: &'a Pattern,
    pub start_id: HepNodeId,
    pub graph: &'b HepGraph,
}
impl<'a, 'b> HepMatcher<'a, 'b> {
    pub fn new(pattern: &'a Pattern, start_id: HepNodeId, graph: &'b HepGraph) -> Self {
        Self {
            pattern,
            start_id,
            graph,
        }
    }

    pub fn match_pattern(&self) -> bool {
        let operator = self.graph.operator(self.start_id).unwrap();
        if !(self.pattern.predicate)(operator) {
            return false;
        }

        match &self.pattern.children {
            PatternChildrenPredicate::MatchedRecursive => {
                for node_id in self
                    .graph
                    .node_iter(HepMatchOrder::TopDown, Some(self.start_id))
                {
                    if !(self.pattern.predicate)(self.graph.operator(node_id).unwrap()) {
                        return false;
                    }
                }
            }
            PatternChildrenPredicate::Predicate(patterns) => {
                for node_id in self.graph.children_at(self.start_id) {
                    for pattern in patterns {
                        let matcher = HepMatcher::new(pattern, node_id, self.graph);
                        if !matcher.match_pattern() {
                            return false;
                        }
                    }
                }
            }
            PatternChildrenPredicate::None => {}
        }
        true
    }
}

mod tests {
    use std::sync::Arc;

    use crate::planner::logical_plan::LogicalPlan;
    use crate::planner::operator::LogicalOperator;

    #[test]
    pub fn test_hep_matcher_with_matched_recursive_pattern() {
        let logical_plan = LogicalPlan {
            operator: LogicalOperator::Dummy,
            children: vec![
                Arc::new(LogicalPlan {
                    operator: LogicalOperator::Dummy,
                    children: vec![],
                }),
                Arc::new(LogicalPlan {
                    operator: LogicalOperator::Dummy,
                    children: vec![],
                }),
            ],
        };

        let graph = super::HepGraph::new(Arc::new(logical_plan));
        let matcher = super::HepMatcher::new(
            &super::Pattern {
                predicate: |operator| match operator {
                    LogicalOperator::Dummy => true,
                    _ => false,
                },
                children: super::PatternChildrenPredicate::MatchedRecursive,
            },
            graph.root,
            &graph,
        );

        assert!(matcher.match_pattern());
    }
}
