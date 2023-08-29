use std::sync::{Arc, Mutex};

use crate::{
    dbtype::value::Value,
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::operator::PhysicalPlanV2,
    storage::tuple::Tuple,
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanNestedLoopJoinExecutor {
    left_tuple: Mutex<Option<Tuple>>,
}
impl VolcanNestedLoopJoinExecutor {
    pub fn new() -> Self {
        Self {
            left_tuple: Mutex::new(None),
        }
    }
}
impl VolcanoExecutor for VolcanNestedLoopJoinExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::NestedLoopJoin(op) = op.as_ref() {
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
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::NestedLoopJoin(op) = op.as_ref() {
            let left_executor = children[0].clone();
            let right_executor = children[1].clone();

            let left_tuple = self.left_tuple.lock().unwrap();
            let mut left_next_result = if left_tuple.is_none() {
                left_executor.next(context)
            } else {
                NextResult::new(Some(left_tuple.clone().unwrap()), false)
            };
            // release mutex
            drop(left_tuple);

            while !left_next_result.exhausted && left_next_result.tuple.is_some() {
                let left_tuple = left_next_result.tuple.clone().unwrap();

                let mut right_next_result = right_executor.next(context);
                while !right_next_result.exhausted && right_next_result.tuple.is_some() {
                    let right_tuple = right_next_result.tuple.unwrap();

                    // TODO judge if matches
                    if op.condition.is_none() {
                        // save latest left_next_result before return
                        *self.left_tuple.lock().unwrap() = Some(left_tuple.clone());

                        return NextResult::new(
                            Some(Tuple::from_tuples(vec![
                                (left_tuple, left_executor.operator.output_schema()),
                                (right_tuple, right_executor.operator.output_schema()),
                            ])),
                            false,
                        );
                    } else {
                        let condition = op.condition.clone().unwrap();
                        let evaluate_res = condition.evaluate_join(
                            &left_tuple,
                            &left_executor.operator.output_schema(),
                            &right_tuple,
                            &right_executor.operator.output_schema(),
                        );
                        // TODO support left/right join after null support added
                        if let Value::Boolean(v) = evaluate_res {
                            if v {
                                // save latest left_next_result before return
                                *self.left_tuple.lock().unwrap() = Some(left_tuple.clone());

                                return NextResult::new(
                                    Some(Tuple::from_tuples(vec![
                                        (left_tuple, left_executor.operator.output_schema()),
                                        (right_tuple, right_executor.operator.output_schema()),
                                    ])),
                                    false,
                                );
                            }
                        } else {
                            panic!("nested loop join condition should be boolean")
                        }
                    }

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
