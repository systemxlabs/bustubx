use std::sync::Arc;

use crate::planner::{logical_plan::LogicalPlan, operator::LogicalOperator};

use self::physical_plan::PhysicalPlan;

pub mod operator;
pub mod physical_plan;

pub struct Optimizer {
    logical_plan: Arc<LogicalPlan>,
}
impl Optimizer {
    pub fn new(logical_plan: Arc<LogicalPlan>) -> Self {
        Self { logical_plan }
    }

    // 生成优化后的物理计划
    pub fn find_best(&self) -> PhysicalPlan {
        let physical_node = Self::build_physical_node(
            self.logical_plan.clone(),
            self.logical_plan.children.clone(),
        );
        // TODO 递归
        Self::build_physical_plan(physical_node, self.logical_plan.clone())
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
                PhysicalPlan::new_insert_node(&logic_insert.table_name, &logic_insert.columns)
            }
            LogicalOperator::Values(ref logical_values) => {
                PhysicalPlan::new_values_node(&logical_values.columns, &logical_values.tuples)
            }
            LogicalOperator::Project(ref logical_project) => {
                PhysicalPlan::new_project_node(&logical_project.expressions)
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
            _ => unimplemented!(),
        }
    }
}
