use petgraph::stable_graph::{NodeIndex, StableDiGraph};

use super::opt_expr::OptExprNodeId;

pub type HepNodeId = NodeIndex<OptExprNodeId>;

pub struct HepNode {
    id: HepNodeId,
    // plan:
}

pub struct HepGraph {
    graph: StableDiGraph<HepNode, usize, usize>,
    root: HepNodeId,
}
