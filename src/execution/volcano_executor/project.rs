use crate::execution::execution_plan::ExecutionPlan;
use crate::{
    execution::ExecutionContext, optimizer::physical_plan_v2::PhysicalPlanV2, storage::tuple::Tuple,
};
use std::sync::Arc;

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoProjectExecutor;
impl VolcanoExecutor for VolcanoProjectExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::Project(op) = op.as_ref() {
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
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::Project(op) = op.as_ref() {
            let child = children[0].clone();
            let next_result = child.next(context);
            if next_result.tuple.is_none() {
                return NextResult::new(None, next_result.exhausted);
            }
            let mut new_values = Vec::new();
            for expr in &op.expressions {
                new_values.push(expr.evaluate(
                    next_result.tuple.as_ref(),
                    Some(&child.operator.output_schema()),
                ));
            }
            NextResult::new(Some(Tuple::from_values(new_values)), next_result.exhausted)
        } else {
            panic!("not project operator")
        }
    }
}
