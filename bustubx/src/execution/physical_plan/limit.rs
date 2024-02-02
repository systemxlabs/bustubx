use std::sync::{atomic::AtomicU32, Arc};
use tracing::debug;

use crate::catalog::SchemaRef;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: usize,
    pub input: Arc<PhysicalPlan>,

    cursor: AtomicU32,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: usize, input: Arc<PhysicalPlan>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
            cursor: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalLimit {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init limit executor");
        self.cursor.store(0, std::sync::atomic::Ordering::SeqCst);
        self.input.init(context)
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        loop {
            let next_tuple = self.input.next(context)?;
            if next_tuple.is_none() {
                return Ok(None);
            }
            let cursor = self
                .cursor
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let offset = self.offset;
            if (cursor as usize) < offset {
                continue;
            }
            if self.limit.is_some() {
                let limit = self.limit.unwrap();
                if (cursor as usize) < offset + limit {
                    return Ok(next_tuple);
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(next_tuple);
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
