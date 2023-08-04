use crate::execution::execution_plan::ExecutionPlan;
use crate::{
    execution::ExecutionContext, optimizer::operator::PhysicalOperator, storage::tuple::Tuple,
};
use std::sync::Arc;

use super::VolcanoExecutor;

#[derive(Debug)]
pub struct VolcanoFilterExecutor;
impl VolcanoExecutor for VolcanoFilterExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::Filter(op) = op.as_ref() {
            println!("init filter executor");
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not filter operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple> {
        todo!()
    }
}
