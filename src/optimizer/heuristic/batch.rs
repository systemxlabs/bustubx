use super::rule::Rule;

// A batch of rules
#[derive(Debug, Clone)]
pub struct HepBatch {
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub rules: Vec<Box<dyn Rule>>,
}
impl HepBatch {
    pub fn new(name: &str, strategy: HepBatchStrategy, rules: Vec<Box<dyn Rule>>) -> Self {
        Self {
            name: name.to_string(),
            strategy,
            rules,
        }
    }
}

#[derive(Debug, Clone)]
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
