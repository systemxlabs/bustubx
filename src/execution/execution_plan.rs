use std::sync::Arc;

use crate::{optimizer::operator::PhysicalOperator, storage::tuple::Tuple};

use super::volcano_executor::{
    insert::VolcanoInsertExecutor, values::VolcanValuesExecutor, VolcanoExecutor,
};

#[derive(Debug)]
pub enum Executor {
    Dummy,
    VolcanoInsert(VolcanoInsertExecutor),
    VolcanoValues(VolcanValuesExecutor),
}

#[derive(Debug)]
pub struct ExecutionPlan {
    pub executor: Executor,
    pub operator: Arc<PhysicalOperator>,
    pub children: Vec<Arc<ExecutionPlan>>,
}
impl ExecutionPlan {
    pub fn dummy() -> Self {
        Self {
            executor: Executor::Dummy,
            operator: Arc::new(PhysicalOperator::Dummy),
            children: Vec::new(),
        }
    }
    pub fn new_insert_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoInsert(VolcanoInsertExecutor {}),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_values_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoValues(VolcanValuesExecutor::new()),
            operator,
            children: Vec::new(),
        }
    }
    pub fn next(&self) -> Option<Tuple> {
        match self.executor {
            Executor::Dummy => None,
            Executor::VolcanoInsert(ref executor) => {
                executor.next(self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoValues(ref executor) => {
                executor.next(self.operator.clone(), self.children.clone())
            }
        }
    }
}
