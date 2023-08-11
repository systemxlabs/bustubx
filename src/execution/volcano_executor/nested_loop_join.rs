use std::sync::{atomic::AtomicUsize, Arc, Mutex};

use crate::{
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::{operator::PhysicalOperator, physical_plan::PhysicalPlan},
    storage::tuple::Tuple,
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanNestedLoopJoinExecutor {}
impl VolcanNestedLoopJoinExecutor {
    pub fn new() -> Self {
        Self {}
    }
}
impl VolcanoExecutor for VolcanNestedLoopJoinExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::NestedLoopJoin(op) = op.as_ref() {
            println!("init nested loop join executor");
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not nested loop join operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalOperator::NestedLoopJoin(op) = op.as_ref() {
            let left_executor = children[0].clone();
            let right_executor = children[1].clone();

            let mut left_next_result = left_executor.next(context);
            while !left_next_result.exhausted && left_next_result.tuple.is_some() {
                let left_tuple = left_next_result.tuple.unwrap();

                let mut right_next_result = right_executor.next(context);
                while !right_next_result.exhausted && right_next_result.tuple.is_some() {
                    let right_tuple = right_next_result.tuple.unwrap();

                    // TODO judge if matches
                    return NextResult::new(
                        Some(Tuple::from_tuples(vec![
                            (left_tuple, left_executor.operator.output_schema()),
                            (right_tuple, right_executor.operator.output_schema()),
                        ])),
                        false,
                    );

                    right_next_result = right_executor.next(context);
                }

                // reset right executor
                right_executor.init(context);
                left_next_result = left_executor.next(context);
            }
            return NextResult::new(None, true);
        } else {
            panic!("not nested loop join operator")
        }
    }
}
