use std::sync::Arc;

use crate::{optimizer::operator::PhysicalOperator, storage::tuple::Tuple};

use super::{execution_plan::ExecutionPlan, ExecutionContext};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod project;
pub mod table_scan;
pub mod values;

pub trait VolcanoExecutor {
    fn init(&mut self);
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple>;
}
