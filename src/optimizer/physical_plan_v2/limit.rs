use std::sync::{atomic::AtomicU32, Arc};

use crate::{
    catalog::schema::Schema,
    execution::{ExecutionContext, VolcanoExecutorV2},
    storage::tuple::Tuple,
};

use super::PhysicalPlanV2;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub input: Arc<PhysicalPlanV2>,

    cursor: AtomicU32,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: Option<usize>, input: Arc<PhysicalPlanV2>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
            cursor: AtomicU32::new(0),
        }
    }
    pub fn output_schema(&self) -> Schema {
        return self.input.output_schema();
    }
}
impl VolcanoExecutorV2 for PhysicalLimit {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init limit executor");
        self.cursor.store(0, std::sync::atomic::Ordering::SeqCst);
        self.input.init(context);
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        loop {
            let next_tuple = self.input.next(context);
            if next_tuple.is_none() {
                return None;
            }
            let cursor = self
                .cursor
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let offset = self.offset.unwrap_or(0);
            if (cursor as usize) < offset {
                continue;
            }
            if self.limit.is_some() {
                let limit = self.limit.unwrap();
                if (cursor as usize) < offset + limit {
                    return next_tuple;
                } else {
                    return None;
                }
            } else {
                return next_tuple;
            }
        }
    }
}
