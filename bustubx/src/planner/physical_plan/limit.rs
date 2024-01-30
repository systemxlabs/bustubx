use std::sync::{atomic::AtomicU32, Arc};

use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub input: Arc<PhysicalPlan>,

    cursor: AtomicU32,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: Option<usize>, input: Arc<PhysicalPlan>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
            cursor: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalLimit {
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

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}
