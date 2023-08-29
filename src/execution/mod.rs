use std::sync::Arc;

use crate::{
    catalog::catalog::Catalog,
    optimizer::{physical_plan::PhysicalPlan, physical_plan_v2::PhysicalPlanV2},
    storage::tuple::Tuple,
};

use self::execution_plan::ExecutionPlan;

pub mod execution_plan;
pub mod volcano_executor;

pub trait VolcanoExecutorV2 {
    fn init(&self, context: &mut ExecutionContext);
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple>;
}

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
    pub fn execute(&mut self, plan: ExecutionPlan) -> Vec<Tuple> {
        plan.init(&mut self.context);
        let mut result = Vec::new();
        loop {
            let next_result = plan.next(&mut self.context);
            if next_result.tuple.is_some() {
                result.push(next_result.tuple.unwrap());
            }
            if next_result.exhausted {
                break;
            }
        }
        result
    }

    pub fn execute_v2(&mut self, plan: Arc<PhysicalPlanV2>) -> Vec<Tuple> {
        plan.init(&mut self.context);
        let mut result = Vec::new();
        loop {
            let next_tuple = plan.next(&mut self.context);
            if next_tuple.is_some() {
                result.push(next_tuple.unwrap());
            } else {
                break;
            }
        }
        result
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
            PhysicalPlanV2::Dummy => ExecutionPlan::dummy(),
            PhysicalPlanV2::CreateTable(_) => {
                ExecutionPlan::new_create_table_node(physical_operator)
            }
            PhysicalPlanV2::Insert(_) => ExecutionPlan::new_insert_node(physical_operator),
            PhysicalPlanV2::Values(_) => ExecutionPlan::new_values_node(physical_operator),
            PhysicalPlanV2::TableScan(_) => ExecutionPlan::new_table_scan_node(physical_operator),
            PhysicalPlanV2::Filter(_) => ExecutionPlan::new_filter_node(physical_operator),
            PhysicalPlanV2::Project(_) => ExecutionPlan::new_project_node(physical_operator),
            PhysicalPlanV2::Limit(_) => ExecutionPlan::new_limit_node(physical_operator),
            PhysicalPlanV2::NestedLoopJoin(_) => {
                ExecutionPlan::new_nested_loop_join_node(physical_operator)
            }
            _ => unimplemented!(),
        }
    }
}
