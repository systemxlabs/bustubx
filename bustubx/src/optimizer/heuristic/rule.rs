use std::fmt::Debug;

use super::{
    graph::{HepGraph, HepNodeId},
    pattern::Pattern,
};

pub trait Rule: Debug + RuleClone {
    fn pattern(&self) -> &Pattern;
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool;
}

/// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
pub trait RuleClone {
    fn clone_box(&self) -> Box<dyn Rule>;
}

impl<T> RuleClone for T
where
    T: 'static + Rule + Clone,
{
    fn clone_box(&self) -> Box<dyn Rule> {
        Box::new(self.clone())
    }
}
impl Clone for Box<dyn Rule> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
