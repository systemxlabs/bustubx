use log::debug;
use std::sync::Mutex;

use crate::catalog::SchemaRef;
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
            iterator: Mutex::new(TableIterator::new(None, None)),
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init table scan executor");
        let table_info = context.catalog.table_mut(&self.table)?;
        let inited_iterator = table_info.table.iter(None, None);
        let mut iterator = self.iterator.lock().unwrap();
        *iterator = inited_iterator;
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let table_info = context.catalog.table_mut(&self.table)?;
        let mut iterator = self.iterator.lock().unwrap();
        let full_tuple = iterator.next(&mut table_info.table);
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
