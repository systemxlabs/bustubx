use std::fmt::Debug;

use super::{
    graph::{HepGraph, HepNodeId},
    pattern::Pattern,
};

pub trait Rule: Debug {
    fn pattern(&self) -> &Pattern;
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool;
}
