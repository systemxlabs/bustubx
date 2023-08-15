use std::sync::Arc;

use petgraph::{
    stable_graph::{NodeIndex, StableDiGraph},
    visit::Bfs,
};

use crate::planner::{logical_plan::LogicalPlan, operator::LogicalOperator};

use super::batch::HepMatchOrder;

pub type HepNodeId = NodeIndex<usize>;

pub struct HepNode {
    id: HepNodeId,
    operator: LogicalOperator,
}

pub struct HepGraph {
    graph: StableDiGraph<HepNode, usize, usize>,
    root: HepNodeId,
}
impl HepGraph {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        let mut graph = StableDiGraph::<HepNode, usize, usize>::default();
        let root_node_id = Self::add_node_recursively(&mut graph, plan);

        Self {
            graph,
            root: root_node_id,
        }
    }

    fn add_node_recursively(
        graph: &mut StableDiGraph<HepNode, usize, usize>,
        plan: Arc<LogicalPlan>,
    ) -> HepNodeId {
        let parent_node_id = graph.add_node(HepNode {
            // fake id for now, will be updated later
            id: HepNodeId::default(),
            operator: plan.operator.clone(),
        });
        graph[parent_node_id].id = parent_node_id;

        for (order, child) in plan.children.iter().enumerate() {
            let child_node_id = Self::add_node_recursively(graph, child.clone());
            graph.add_edge(parent_node_id, child_node_id, order);
        }

        parent_node_id
    }

    /// If input node is join, we use the edge weight to control the join chilren order.
    pub fn children_at(&self, node_id: HepNodeId) -> Vec<HepNodeId> {
        let mut children = self
            .graph
            .neighbors_directed(node_id, petgraph::Direction::Outgoing)
            .collect::<Vec<_>>();
        if children.len() > 1 {
            children.sort_by(|a, b| {
                let a_edge = self.graph.find_edge(node_id, *a).unwrap();
                let a_weight = self.graph.edge_weight(a_edge).unwrap();
                let b_edge = self.graph.find_edge(node_id, *b).unwrap();
                let b_weight = self.graph.edge_weight(b_edge).unwrap();
                a_weight.cmp(b_weight)
            })
        }
        children
    }

    pub fn node(&self, node_id: HepNodeId) -> Option<&HepNode> {
        self.graph.node_weight(node_id)
    }

    /// Traverse the graph in breadth first search order.
    pub fn bfs(&self, start: HepNodeId) -> Vec<HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut iter = Bfs::new(&self.graph, start);
        while let Some(node_id) = iter.next(&self.graph) {
            ids.push(node_id);
        }
        ids
    }

    pub fn node_iter(
        &self,
        order: HepMatchOrder,
        start: Option<HepNodeId>,
    ) -> Box<dyn Iterator<Item = HepNodeId>> {
        let ids = self.bfs(start.unwrap_or(self.root));
        match order {
            HepMatchOrder::TopDown => Box::new(ids.into_iter()),
            HepMatchOrder::BottomUp => Box::new(ids.into_iter().rev()),
        }
    }
}

mod tests {
    use std::sync::Arc;

    use petgraph::adj::EdgeIndex;

    use crate::{
        binder::statement::insert,
        catalog::column::{Column, DataType},
        database::Database,
        dbtype::value::Value,
        optimizer::heuristic::graph::{HepNode, HepNodeId},
        planner::{
            logical_plan::{self, LogicalPlan},
            operator::LogicalOperator,
        },
    };

    #[test]
    pub fn test_hep_graph_new() {
        let db_path = "test_hep_graph_new.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        // 0: project
        //   1: join
        //     2: scan t1
        //     3: scan t2
        let graph = super::HepGraph::new(Arc::new(logical_plan));
        assert_eq!(graph.graph.node_count(), 4);
        assert_eq!(graph.graph.edge_count(), 3);

        let node = graph.graph.node_weight(HepNodeId::new(0)).unwrap();
        assert!(matches!(&node.operator, LogicalOperator::Project(_)));

        let node = graph.graph.node_weight(HepNodeId::new(1)).unwrap();
        assert!(matches!(&node.operator, LogicalOperator::Join(_)));

        let node = graph.graph.node_weight(HepNodeId::new(2)).unwrap();
        assert!(matches!(&node.operator, LogicalOperator::Scan(_)));

        let node = graph.graph.node_weight(HepNodeId::new(3)).unwrap();
        assert!(matches!(&node.operator, LogicalOperator::Scan(_)));

        let edge = graph.graph.find_edge(HepNodeId::new(0), HepNodeId::new(1));
        assert!(edge.is_some());
        assert_eq!(*graph.graph.edge_weight(edge.unwrap()).unwrap(), 0);

        let edge = graph.graph.find_edge(HepNodeId::new(1), HepNodeId::new(2));
        assert!(edge.is_some());
        assert_eq!(*graph.graph.edge_weight(edge.unwrap()).unwrap(), 0);

        let edge = graph.graph.find_edge(HepNodeId::new(1), HepNodeId::new(3));
        assert!(edge.is_some());
        assert_eq!(*graph.graph.edge_weight(edge.unwrap()).unwrap(), 1);

        let edge = graph.graph.find_edge(HepNodeId::new(2), HepNodeId::new(3));
        assert!(edge.is_none());

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_hep_graph_bfs() {
        let db_path = "test_hep_graph_bfs.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let graph = super::HepGraph::new(Arc::new(logical_plan));
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], HepNodeId::new(0));
        assert_eq!(ids[1], HepNodeId::new(1));
        assert!(ids[2] == HepNodeId::new(2) || ids[2] == HepNodeId::new(3));
        assert!(ids[3] == HepNodeId::new(2) || ids[3] == HepNodeId::new(3));

        let _ = std::fs::remove_file(db_path);
    }
}
