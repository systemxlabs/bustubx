use std::sync::{atomic::AtomicUsize, Arc, Mutex};

use crate::{
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::{operator::PhysicalPlanV2, physical_plan::PhysicalPlan},
    storage::tuple::Tuple,
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanLimitExecutor {
    cursor: Mutex<usize>,
}
impl VolcanLimitExecutor {
    pub fn new() -> Self {
        Self {
            cursor: Mutex::new(0),
        }
    }
}
impl VolcanoExecutor for VolcanLimitExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::Limit(op) = op.as_ref() {
            println!("init limit executor");
            let mut cursor = self.cursor.lock().unwrap();
            *cursor = 0;
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not limit operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::Limit(op) = op.as_ref() {
            let mut cursor = self.cursor.lock().unwrap();
            let child = children[0].clone();
            let next_result = child.next(context);
            if next_result.exhausted {
                return NextResult::new(None, next_result.exhausted);
            }
            if next_result.tuple.is_some() {
                let offset = op.offset.unwrap_or(0);
                if *cursor < offset {
                    *cursor += 1;
                    return NextResult::new(None, false);
                }
                if op.limit.is_some() {
                    let limit = op.limit.unwrap();
                    if *cursor < offset + limit {
                        *cursor += 1;
                        return NextResult::new(next_result.tuple, false);
                    } else {
                        // 超过limit，exauhsted返回true
                        *cursor += 1;
                        return NextResult::new(None, true);
                    }
                } else {
                    *cursor += 1;
                    return NextResult::new(next_result.tuple, false);
                }
            } else {
                return NextResult::new(None, next_result.exhausted);
            }
        } else {
            panic!("not limit operator")
        }
    }
}
