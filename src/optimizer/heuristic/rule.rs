use std::fmt::Debug;

use super::{pattern::Pattern, graph::{HepGraph, HepNodeId}};

pub trait Rule: Debug {
    fn pattern(&self) -> &Pattern;
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool;
}