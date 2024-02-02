use crate::catalog::SchemaRef;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::{BustubxResult, Tuple};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct PhysicalEmpty {
    pub produce_row_count: usize,
    pub schema: SchemaRef,
    outputted_count: AtomicUsize,
}

impl PhysicalEmpty {
    pub fn new(produce_row_count: usize, schema: SchemaRef) -> Self {
        Self {
            produce_row_count,
            schema,
            outputted_count: AtomicUsize::new(0),
        }
    }
}

impl VolcanoExecutor for PhysicalEmpty {
    fn init(&self, _context: &mut ExecutionContext) -> BustubxResult<()> {
        self.outputted_count.store(0, Ordering::SeqCst);
        Ok(())
    }
    fn next(&self, _context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        if self.outputted_count.fetch_add(1, Ordering::SeqCst) < self.produce_row_count {
            Ok(Some(Tuple::new(self.schema.clone(), vec![])))
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Empty")
    }
}
