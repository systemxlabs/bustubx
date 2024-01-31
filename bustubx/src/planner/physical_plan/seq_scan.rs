use std::sync::{Arc, Mutex};

use crate::catalog::{ColumnRef, SchemaRef};
use crate::{
    catalog::{Schema, TableOid},
    execution::{ExecutionContext, VolcanoExecutor},
    storage::{TableIterator, Tuple},
    BustubxResult,
};

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table_oid: TableOid,
    pub columns: Vec<ColumnRef>,

    iterator: Mutex<TableIterator>,
}

impl PhysicalSeqScan {
    pub fn new(table_oid: TableOid, columns: Vec<ColumnRef>) -> Self {
        PhysicalSeqScan {
            table_oid,
            columns,
            iterator: Mutex::new(TableIterator::new(None, None)),
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        println!("init table scan executor");
        let table_info = context
            .catalog
            .get_mut_table_by_oid(self.table_oid)
            .unwrap();
        let inited_iterator = table_info.table.iter(None, None);
        let mut iterator = self.iterator.lock().unwrap();
        *iterator = inited_iterator;
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let table_info = context
            .catalog
            .get_mut_table_by_oid(self.table_oid)
            .unwrap();
        let mut iterator = self.iterator.lock().unwrap();
        let full_tuple = iterator.next(&mut table_info.table);
        return Ok(full_tuple.map(|t| t.1));
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema {
            columns: self.columns.clone(),
        })
    }
}

impl std::fmt::Display for PhysicalSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
