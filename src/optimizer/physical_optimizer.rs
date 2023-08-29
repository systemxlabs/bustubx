use std::sync::Arc;

use crate::planner::{
    logical_plan::{self, LogicalPlan},
    operator::LogicalOperator,
};

use super::physical_plan::PhysicalPlan;

pub struct PhysicalOptimizer {}
impl PhysicalOptimizer {
    // output optimized physical plan
    pub fn find_best(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        // TODO optimization
        let logical_plan = Arc::new(logical_plan);
        let physical_node =
            Self::build_physical_node(logical_plan.clone(), logical_plan.children.clone());
        Self::build_physical_plan(physical_node, logical_plan.clone())
    }

    fn build_physical_plan(
        mut physical_plan: PhysicalPlan,
        logical_plan: Arc<LogicalPlan>,
    ) -> PhysicalPlan {
        for logical_child in logical_plan.children.iter() {
            let physical_child =
                Self::build_physical_node(logical_child.clone(), logical_child.children.clone());
            physical_plan
                .children
                .push(Arc::new(Self::build_physical_plan(
                    physical_child,
                    logical_child.clone(),
                )));
        }
        physical_plan
    }

    fn build_physical_node(
        logical_node: Arc<LogicalPlan>,
        logical_node_children: Vec<Arc<LogicalPlan>>,
    ) -> PhysicalPlan {
        match logical_node.operator {
            LogicalOperator::Dummy => PhysicalPlan::dummy(),
            LogicalOperator::CreateTable(ref logic_create_table) => {
                PhysicalPlan::new_create_table_node(
                    &logic_create_table.table_name,
                    &logic_create_table.schema,
                )
            }
            LogicalOperator::Insert(ref logic_insert) => {
                let child_logical_node = logical_node_children[0].clone();
                let child_physical_node = Self::build_physical_node(
                    child_logical_node.clone(),
                    child_logical_node.children.clone(),
                );
                PhysicalPlan::new_insert_node(
                    &logic_insert.table_name,
                    &logic_insert.columns,
                    child_physical_node.operator.clone(),
                )
            }
            LogicalOperator::Values(ref logical_values) => {
                PhysicalPlan::new_values_node(&logical_values.columns, &logical_values.tuples)
            }
            LogicalOperator::Project(ref logical_project) => {
                let child_logical_node = logical_node_children[0].clone();
                let child_physical_node = Self::build_physical_node(
                    child_logical_node.clone(),
                    child_logical_node.children.clone(),
                );
                PhysicalPlan::new_project_node(
                    &logical_project.expressions,
                    child_physical_node.operator.clone(),
                )
            }
            LogicalOperator::Filter(ref logical_filter) => {
                // filter下只有一个子节点
                let child_logical_node = logical_node_children[0].clone();
                let child_physical_node = Self::build_physical_node(
                    child_logical_node.clone(),
                    child_logical_node.children.clone(),
                );
                PhysicalPlan::new_filter_node(
                    &logical_filter.predicate,
                    child_physical_node.operator.clone(),
                )
            }
            LogicalOperator::Scan(ref logical_table_scan) => PhysicalPlan::new_table_scan_node(
                &logical_table_scan.table_oid,
                &logical_table_scan.columns,
            ),
            LogicalOperator::Limit(ref logical_limit) => {
                // limit下只有一个子节点
                let child_logical_node = logical_node_children[0].clone();
                let child_physical_node = Self::build_physical_node(
                    child_logical_node.clone(),
                    child_logical_node.children.clone(),
                );
                PhysicalPlan::new_limit_node(
                    &logical_limit.limit,
                    &logical_limit.offset,
                    child_physical_node.operator.clone(),
                )
            }
            LogicalOperator::Join(ref logical_join) => {
                let left_logical_node = logical_node_children[0].clone();
                let right_logical_node = logical_node_children[1].clone();
                let left_physical_node = Self::build_physical_node(
                    left_logical_node.clone(),
                    left_logical_node.children.clone(),
                );
                let right_physical_node = Self::build_physical_node(
                    right_logical_node.clone(),
                    right_logical_node.children.clone(),
                );
                PhysicalPlan::new_nested_loop_join_node(
                    logical_join.join_type,
                    logical_join.condition.clone(),
                    left_physical_node.operator.clone(),
                    right_physical_node.operator.clone(),
                )
            }
            _ => unimplemented!(),
        }
    }
}
