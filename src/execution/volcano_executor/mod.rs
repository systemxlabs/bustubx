use std::sync::Arc;

use crate::{optimizer::operator::PhysicalOperator, storage::tuple::Tuple};

use super::execution_plan::ExecutionPlan;

pub mod insert;
pub mod values;

pub trait VolcanoExecutor {
    fn init(&mut self);
    fn next(&self, op: Arc<PhysicalOperator>, children: Vec<Arc<ExecutionPlan>>) -> Option<Tuple>;
}
