use super::rule::Rule;

// A batch of rules
#[derive(Debug)]
pub struct HepBatch {
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub rules: Vec<Box<dyn Rule>>,
}

#[derive(Debug)]
pub struct HepBatchStrategy {
    pub max_iteration: usize,
    /// An order to traverse the plan tree nodes
    pub match_order: HepMatchOrder,
}
impl HepBatchStrategy {
    pub fn once_topdown() -> Self {
        Self {
            max_iteration: 1,
            match_order: HepMatchOrder::TopDown,
        }
    }

    pub fn fix_point_topdown(max_iteration: usize) -> Self {
        Self {
            max_iteration,
            match_order: HepMatchOrder::TopDown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HepMatchOrder {
    /// Match from root down. A match attempt at an ancestor always precedes all match attempts at
    /// its descendants.
    TopDown,
    /// Match from leaves up. A match attempt at a descendant precedes all match attempts at its
    /// ancestors.
    BottomUp,
}
