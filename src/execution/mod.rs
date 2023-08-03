use std::sync::Arc;

use crate::optimizer::{operator::PhysicalOperator, physical_plan::PhysicalPlan};

use self::execution_plan::ExecutionPlan;

pub mod execution_plan;
pub mod volcano_executor;

pub struct ExecutionEngine {}
impl ExecutionEngine {
    pub fn execute(&mut self, plan: ExecutionPlan) {
        loop {
            let tuple = plan.next();
            if tuple.is_some() {
                println!("tuple: {:?}", tuple.unwrap());
            } else {
                break;
            }
        }
    }

    // 生成执行计划
    pub fn plan(&mut self, plan: Arc<PhysicalPlan>) -> ExecutionPlan {
        let execution_node = Self::build_execution_node(plan.clone());
        Self::build_execution_plan(execution_node, plan.clone())
    }

    fn build_execution_plan(
        mut execution_plan: ExecutionPlan,
        physical_node: Arc<PhysicalPlan>,
    ) -> ExecutionPlan {
        for physical_child in physical_node.children.iter() {
            let execution_child = Self::build_execution_node(physical_child.clone());
            execution_plan
                .children
                .push(Arc::new(Self::build_execution_plan(
                    execution_child,
                    physical_child.clone(),
                )));
        }
        execution_plan
    }

    fn build_execution_node(physical_node: Arc<PhysicalPlan>) -> ExecutionPlan {
        let physical_operator = physical_node.operator.clone();
        match physical_node.operator.as_ref() {
            PhysicalOperator::Dummy => ExecutionPlan::dummy(),
            PhysicalOperator::Insert(_) => ExecutionPlan::new_insert_node(physical_operator),
            PhysicalOperator::Values(_) => ExecutionPlan::new_values_node(physical_operator),
        }
    }
}
