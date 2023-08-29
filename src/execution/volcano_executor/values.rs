use std::sync::{atomic::AtomicUsize, Arc, Mutex};

use crate::{
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::{physical_plan::PhysicalPlan, physical_plan_v2::PhysicalPlanV2},
    storage::tuple::Tuple,
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanValuesExecutor {
    cursor: Mutex<usize>,
}
impl VolcanValuesExecutor {
    pub fn new() -> Self {
        Self {
            cursor: Mutex::new(0),
        }
    }
}
impl VolcanoExecutor for VolcanValuesExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::Values(op) = op.as_ref() {
            println!("init values executor");
            let mut cursor = self.cursor.lock().unwrap();
            *cursor = 0;
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not values operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        _children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::Values(op) = op.as_ref() {
            let mut cursor = self.cursor.lock().unwrap();
            if *cursor < op.tuples.len() {
                let values = op.tuples[*cursor].clone();
                *cursor += 1;
                NextResult::new(Some(Tuple::from_values(values)), false)
            } else {
                NextResult::new(None, true)
            }
        } else {
            panic!("not values operator")
        }
    }
}
