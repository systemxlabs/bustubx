use super::{
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

    pub fn match_pattern(&self) -> Option<Vec<HepNodeId>> {
        unimplemented!()
    }
}
