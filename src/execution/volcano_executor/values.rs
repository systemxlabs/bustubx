use std::sync::{atomic::AtomicUsize, Arc, Mutex};

use crate::{
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::{operator::PhysicalOperator, physical_plan::PhysicalPlan},
    storage::tuple::Tuple,
};

use super::VolcanoExecutor;

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
    fn init(&mut self) {
        self.cursor = Mutex::new(0);
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        _children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple> {
        if let PhysicalOperator::Values(op) = op.as_ref() {
            let mut cursor = self.cursor.lock().unwrap();
            if *cursor < op.tuples.len() {
                let values = op.tuples[*cursor].clone();
                *cursor += 1;
                Some(Tuple::from_values(values))
            } else {
                None
            }
        } else {
            panic!("not values operator")
        }
    }
}
