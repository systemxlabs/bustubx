use super::rule::Rule;

#[derive(Debug)]
pub struct HepBatch {
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub rules: Vec<Box<dyn Rule>>,
}

#[derive(Debug)]
pub struct HepBatchStrategy {
    pub max_iteration: usize,
    pub match_order: HepMatchOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HepMatchOrder {
    TopDown,
    BottomUp,
}
