use crate::execution::execution_plan::ExecutionPlan;
use crate::{
    execution::ExecutionContext, optimizer::operator::PhysicalOperator, storage::tuple::Tuple,
};
use std::sync::Arc;

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoProjectExecutor;
impl VolcanoExecutor for VolcanoProjectExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::Project(op) = op.as_ref() {
            println!("init project executor");
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not project operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        todo!()
    }
}
