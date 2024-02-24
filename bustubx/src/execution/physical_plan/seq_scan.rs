use log::debug;
use std::ops::{Bound, RangeBounds, RangeFull};
use std::sync::Mutex;

use crate::catalog::SchemaRef;
use crate::common::rid::Rid;
use crate::common::TableReference;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::{TableIterator, Tuple},
    BustubxResult,
};

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,

    iterator: Mutex<TableIterator>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            iterator: Mutex::new(TableIterator::new(
                Bound::Unbounded,
                Bound::Unbounded,
                None,
                false,
                false,
            )),
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.iterator.lock().unwrap() = table_heap.scan(RangeFull);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let table_heap = context.catalog.table_heap(&self.table)?;
        let mut iterator = self.iterator.lock().unwrap();
        let full_tuple = iterator.next(&table_heap);
        Ok(full_tuple.map(|t| t.1))
    }

    fn output_schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

impl std::fmt::Display for PhysicalSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqScan")
    }
}
