use std::sync::Arc;

use crate::{
    catalog::catalog::Catalog,
    optimizer::{operator::PhysicalOperator, physical_plan::PhysicalPlan},
};

use self::execution_plan::ExecutionPlan;

pub mod execution_plan;
pub mod volcano_executor;

pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
}
impl ExecutionContext<'_> {
    pub fn new(catalog: &mut Catalog) -> ExecutionContext {
        ExecutionContext { catalog }
    }
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl ExecutionEngine<'_> {
    pub fn execute(&mut self, plan: ExecutionPlan) {
        plan.init(&mut self.context);
        loop {
            let next_result = plan.next(&mut self.context);
            if next_result.tuple.is_some() {
                println!("tuple: {:?}", next_result.tuple.unwrap());
            }
            if next_result.exhausted {
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
            PhysicalOperator::CreateTable(_) => {
                ExecutionPlan::new_create_table_node(physical_operator)
            }
            PhysicalOperator::Insert(_) => ExecutionPlan::new_insert_node(physical_operator),
            PhysicalOperator::Values(_) => ExecutionPlan::new_values_node(physical_operator),
            PhysicalOperator::TableScan(_) => ExecutionPlan::new_table_scan_node(physical_operator),
            PhysicalOperator::Filter(_) => ExecutionPlan::new_filter_node(physical_operator),
            PhysicalOperator::Project(_) => ExecutionPlan::new_project_node(physical_operator),
            PhysicalOperator::Limit(_) => ExecutionPlan::new_limit_node(physical_operator),
            _ => unimplemented!(),
        }
    }
}
