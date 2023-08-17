use std::sync::Arc;

use crate::planner::logical_plan::{self, LogicalPlan};

use self::{
    batch::{HepBatch, HepBatchStrategy},
    graph::{HepGraph, HepNodeId},
    matcher::HepMatcher,
    rule::Rule,
};

pub mod batch;
pub mod graph;
pub mod matcher;
pub mod pattern;
pub mod rule;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    graph: HepGraph,
}
impl HepOptimizer {
    pub fn new(plan: LogicalPlan) -> Self {
        let graph = HepGraph::new(Arc::new(plan));
        Self {
            batches: Vec::new(),
            graph,
        }
    }

    pub fn batch(
        mut self,
        name: &str,
        strategy: HepBatchStrategy,
        rules: Vec<Box<dyn Rule>>,
    ) -> Self {
        self.batches.push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn default_optimizer(plan: LogicalPlan) -> Self {
        // TODO add real rules
        Self::new(plan).batch("test", HepBatchStrategy::fix_point_topdown(10), vec![])
    }

    // output the optimized logical plan
    pub fn find_best(&mut self) -> LogicalPlan {
        for batch in self.batches.clone() {
            let batch_over = false;
            let mut iteration = 0;
            while !batch_over && iteration < batch.strategy.max_iteration {
                if self.apply_batch(&batch) {
                    iteration += 1;
                } else {
                    break;
                }
            }
        }
        self.graph.to_plan()
    }

    fn apply_batch(&mut self, batch: &HepBatch) -> bool {
        let mut applied = false;
        for rule in &batch.rules {
            for node_id in self.graph.node_iter(batch.strategy.match_order, None) {
                if self.apply_rule(rule, node_id) {
                    applied = true;
                    break;
                }
            }
        }
        applied
    }

    fn apply_rule(&mut self, rule: &Box<dyn Rule>, node_id: HepNodeId) -> bool {
        if HepMatcher::new(rule.pattern(), node_id, &self.graph).match_pattern() {
            return rule.apply(node_id, &mut self.graph);
        }
        false
    }
}
