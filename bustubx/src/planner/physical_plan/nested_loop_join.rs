use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::{
    catalog::Schema,
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    planner::table_ref::join::JoinType,
    storage::Tuple,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub left_input: Arc<PhysicalPlan>,
    pub right_input: Arc<PhysicalPlan>,

    left_tuple: Mutex<Option<Tuple>>,
}
impl PhysicalNestedLoopJoin {
    pub fn new(
        join_type: JoinType,
        condition: Option<Expr>,
        left_input: Arc<PhysicalPlan>,
        right_input: Arc<PhysicalPlan>,
    ) -> Self {
        PhysicalNestedLoopJoin {
            join_type,
            condition,
            left_input,
            right_input,
            left_tuple: Mutex::new(None),
        }
    }
}
impl VolcanoExecutor for PhysicalNestedLoopJoin {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init nested loop join executor");
        *self.left_tuple.lock().unwrap() = None;
        self.left_input.init(context);
        self.right_input.init(context);
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        let left_tuple = self.left_tuple.lock().unwrap();
        let mut left_next_tuple = if left_tuple.is_none() {
            self.left_input.next(context)
        } else {
            Some(left_tuple.clone().unwrap())
        };
        // release mutex
        drop(left_tuple);

        while left_next_tuple.is_some() {
            let left_tuple = left_next_tuple.clone().unwrap();

            let mut right_next_tuple = self.right_input.next(context);
            while right_next_tuple.is_some() {
                let right_tuple = right_next_tuple.unwrap();

                // TODO judge if matches
                if self.condition.is_none() {
                    // save latest left_next_result before return
                    *self.left_tuple.lock().unwrap() = Some(left_tuple.clone());

                    return Some(Tuple::try_merge(vec![left_tuple, right_tuple]).unwrap());
                } else {
                    let condition = self.condition.clone().unwrap();
                    let merged_tuple =
                        Tuple::try_merge(vec![left_tuple.clone(), right_tuple.clone()]).unwrap();
                    let evaluate_res = condition.evaluate(&merged_tuple).unwrap();
                    // TODO support left/right join after null support added
                    if let ScalarValue::Boolean(Some(v)) = evaluate_res {
                        if v {
                            // save latest left_next_result before return
                            *self.left_tuple.lock().unwrap() = Some(left_tuple.clone());

                            return Some(Tuple::try_merge(vec![left_tuple, right_tuple]).unwrap());
                        }
                    } else {
                        panic!("nested loop join condition should be boolean")
                    }
                }

                right_next_tuple = self.right_input.next(context);
            }

            // reset right executor
            self.right_input.init(context);
            left_next_tuple = self.left_input.next(context);
        }
        return None;
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(
            Schema::try_merge(vec![
                self.left_input.output_schema().as_ref().clone(),
                self.right_input.output_schema().as_ref().clone(),
            ])
            .unwrap(),
        )
    }
}

impl std::fmt::Display for PhysicalNestedLoopJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
