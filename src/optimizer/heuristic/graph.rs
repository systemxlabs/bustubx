use std::sync::Arc;

use itertools::Itertools;
use petgraph::{
    stable_graph::{NodeIndex, StableDiGraph},
    visit::{Bfs, EdgeRef},
};

use crate::planner::{logical_plan::LogicalPlan, operator::LogicalOperator};

use super::batch::HepMatchOrder;

pub type HepNodeId = NodeIndex<usize>;

pub struct HepNode {
    id: HepNodeId,
    operator: LogicalOperator,
}

pub struct HepGraph {
    pub graph: StableDiGraph<HepNode, usize, usize>,
    pub root: HepNodeId,
    // bump version if graph changed
    pub version: usize,
}
impl HepGraph {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        let mut graph = StableDiGraph::<HepNode, usize, usize>::default();
        let root_node_id = Self::add_node_recursively(&mut graph, plan);

        Self {
            graph,
            root: root_node_id,
            version: 0,
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
        self.graph
            .edges(node_id)
            .sorted_by_key(|edge| edge.weight())
            .map(|edge| edge.target())
            .collect_vec()
    }

    pub fn node(&self, node_id: HepNodeId) -> Option<&HepNode> {
        self.graph.node_weight(node_id)
    }

    pub fn parent_node(&self, node_id: HepNodeId) -> Option<&HepNode> {
        let parent_id = self
            .graph
            .neighbors_directed(node_id, petgraph::Direction::Incoming)
            .next()?;
        self.node(parent_id)
    }

    pub fn operator(&self, node_id: HepNodeId) -> Option<&LogicalOperator> {
        self.node(node_id).map(|node| &node.operator)
    }

    pub fn swap_node(&mut self, a: HepNodeId, b: HepNodeId) {
        let tmp = self.graph[a].operator.clone();
        self.graph[a].operator = std::mem::replace(&mut self.graph[b].operator, tmp);
    }

    pub fn insert_node(
        &mut self,
        parent: HepNodeId,
        child: Option<HepNodeId>,
        operator: LogicalOperator,
    ) {
        let new_node_id = self.graph.add_node(HepNode {
            id: HepNodeId::default(),
            operator,
        });
        self.graph[new_node_id].id = new_node_id;

        let mut order = self.graph.edges(parent).count();

        if let Some(child) = child {
            self.graph.find_edge(parent, child).map(|old_edge_id| {
                order = self.graph.remove_edge(old_edge_id).unwrap();
                self.graph.add_edge(new_node_id, child, 0);
            });
        }

        self.graph.add_edge(parent, new_node_id, order);
    }

    pub fn remove_node(
        &mut self,
        node_id: HepNodeId,
        with_children: bool,
    ) -> Option<LogicalOperator> {
        assert!(node_id != self.root, "cannot remove root node");
        if with_children {
            self.graph.remove_node(node_id).map(|node| node.operator)
        } else {
            let parent_id = self
                .parent_node(node_id)
                .expect("must have parent node if not remove children")
                .id;
            let children = self.children_at(node_id);
            for child in children {
                self.graph.add_edge(parent_id, child, 0);
            }
            self.graph.remove_node(node_id).map(|node| node.operator)
        }
    }

    pub fn replace_node(&mut self, node_id: HepNodeId, operator: LogicalOperator) {
        self.graph[node_id].operator = operator;
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

    pub fn to_plan(&self) -> LogicalPlan {
        self.to_plan_at(self.root)
    }

    fn to_plan_at(&self, start: HepNodeId) -> LogicalPlan {
        let operator = self.operator(start).unwrap().clone();
        let children = self
            .children_at(start)
            .into_iter()
            .map(|child| Arc::new(self.to_plan_at(child)))
            .collect::<Vec<_>>();
        LogicalPlan { operator, children }
    }
}

mod tests {
    use std::sync::Arc;

    use crate::{
        database::Database, optimizer::heuristic::graph::HepNodeId,
        planner::operator::LogicalOperator,
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

        let parent = graph.parent_node(HepNodeId::new(0));
        assert!(parent.is_none());
        let parent = graph.parent_node(HepNodeId::new(1)).unwrap();
        assert_eq!(parent.id, HepNodeId::new(0));
        assert!(matches!(&parent.operator, LogicalOperator::Project(_)));
        let parent = graph.parent_node(HepNodeId::new(2)).unwrap();
        assert_eq!(parent.id, HepNodeId::new(1));
        assert!(matches!(&parent.operator, LogicalOperator::Join(_)));
        let parent = graph.parent_node(HepNodeId::new(3)).unwrap();
        assert_eq!(parent.id, HepNodeId::new(1));
        assert!(matches!(&parent.operator, LogicalOperator::Join(_)));

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

    #[test]
    pub fn test_hep_graph_swap_node() {
        let db_path = "test_hep_graph_swap_node.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let mut graph = super::HepGraph::new(Arc::new(logical_plan));
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], HepNodeId::new(0));
        assert_eq!(ids[1], HepNodeId::new(1));
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Join(_)
        ));

        graph.swap_node(ids[0], ids[1]);
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], HepNodeId::new(0));
        assert_eq!(ids[1], HepNodeId::new(1));
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Join(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Project(_)
        ));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_hep_graph_replace_node() {
        let db_path = "test_hep_graph_replace_node.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let mut graph = super::HepGraph::new(Arc::new(logical_plan));
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Join(_)
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[3]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        let new_op = LogicalOperator::Dummy;
        graph.replace_node(ids[1], new_op);
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Dummy
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[3]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_hep_graph_insert_node() {
        let db_path = "test_hep_graph_insert_node.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let mut graph = super::HepGraph::new(Arc::new(logical_plan));
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Join(_)
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[3]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        graph.insert_node(ids[1], Some(ids[2]), LogicalOperator::Dummy);
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 5);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Join(_)
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Dummy
        ));
        assert!(matches!(
            graph.operator(ids[3]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[4]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_hep_graph_remove_node() {
        let db_path = "test_hep_graph_remove_node.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let mut graph = super::HepGraph::new(Arc::new(logical_plan));
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 4);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Join(_)
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[3]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        graph.remove_node(ids[1], true);
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 1);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));

        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");
        let mut graph = super::HepGraph::new(Arc::new(logical_plan));

        graph.remove_node(HepNodeId::new(1), false);
        let ids = graph.bfs(graph.root);
        assert_eq!(ids.len(), 3);
        assert!(matches!(
            graph.operator(ids[0]).unwrap(),
            LogicalOperator::Project(_)
        ));
        assert!(matches!(
            graph.operator(ids[1]).unwrap(),
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            graph.operator(ids[2]).unwrap(),
            LogicalOperator::Scan(_)
        ));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_hep_graph_to_plan() {
        let db_path = "test_hep_graph_to_plan.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = Database::new_on_disk(db_path);
        db.run("create table t1(a int, b int)");
        db.run("create table t2(a int, b int)");
        let logical_plan = db.build_logical_plan("select * from t1 inner join t2 on t1.a = t2.a");

        let graph = super::HepGraph::new(Arc::new(logical_plan));
        let output_plan = graph.to_plan();
        assert!(matches!(output_plan.operator, LogicalOperator::Project(_)));
        assert_eq!(output_plan.children.len(), 1);
        assert!(matches!(
            output_plan.children[0].operator,
            LogicalOperator::Join(_)
        ));
        assert_eq!(output_plan.children[0].children.len(), 2);
        assert!(matches!(
            output_plan.children[0].children[0].operator,
            LogicalOperator::Scan(_)
        ));
        assert!(matches!(
            output_plan.children[0].children[1].operator,
            LogicalOperator::Scan(_)
        ));

        let _ = std::fs::remove_file(db_path);
    }
}
