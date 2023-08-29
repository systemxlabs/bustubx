use sqlparser::keywords::NO;

use crate::binder::expression::BoundExpression;
use crate::dbtype::value::Value;
use crate::execution::execution_plan::ExecutionPlan;
use crate::{
    execution::ExecutionContext, optimizer::physical_plan_v2::PhysicalPlanV2, storage::tuple::Tuple,
};
use std::sync::Arc;

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoFilterExecutor;
impl VolcanoExecutor for VolcanoFilterExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::Filter(op) = op.as_ref() {
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
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::Filter(op) = op.as_ref() {
            if children.len() != 1 {
                panic!("filter should have only one child")
            }
            let child = children[0].clone();
            let next_result = child.next(context);
            if next_result.tuple.is_none() {
                return NextResult::new(None, next_result.exhausted);
            }
            let tuple = next_result.tuple.unwrap();
            let output_schema = child.operator.output_schema();
            let compare_res = op.predicate.evaluate(Some(&tuple), Some(&output_schema));
            if let Value::Boolean(v) = compare_res {
                if v {
                    return NextResult::new(Some(tuple), next_result.exhausted);
                } else {
                    return NextResult::new(None, next_result.exhausted);
                }
            } else {
                panic!("filter predicate should be boolean")
            }
        } else {
            panic!("not filter operator")
        }
    }
}
