use std::sync::Arc;

use crate::{optimizer::operator::PhysicalOperator, storage::tuple::Tuple};

use super::{execution_plan::ExecutionPlan, ExecutionContext};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod project;
pub mod table_scan;
pub mod values;

pub struct NextResult {
    pub tuple: Option<Tuple>,
    pub exhusted: bool,
}
impl NextResult {
    pub fn new(tuple: Option<Tuple>, exhusted: bool) -> Self {
        Self { tuple, exhusted }
    }
}

pub trait VolcanoExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    );
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult;
}
