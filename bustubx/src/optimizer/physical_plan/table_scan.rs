use std::sync::Mutex;

use crate::{
    catalog::{catalog::TableOid, column::Column, schema::Schema},
    execution::{ExecutionContext, VolcanoExecutor},
    storage::{table_heap::TableIterator, tuple::Tuple},
};

#[derive(Debug)]
pub struct PhysicalTableScan {
    pub table_oid: TableOid,
    pub columns: Vec<Column>,

    iterator: Mutex<TableIterator>,
}
impl PhysicalTableScan {
    pub fn new(table_oid: TableOid, columns: Vec<Column>) -> Self {
        PhysicalTableScan {
            table_oid,
            columns,
            iterator: Mutex::new(TableIterator::new(None, None)),
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(self.columns.clone())
    }
}
impl VolcanoExecutor for PhysicalTableScan {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init table scan executor");
        let table_info = context
            .catalog
            .get_mut_table_by_oid(self.table_oid)
            .unwrap();
        let inited_iterator = table_info.table.iter(None, None);
        let mut iterator = self.iterator.lock().unwrap();
        *iterator = inited_iterator;
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        let table_info = context
            .catalog
            .get_mut_table_by_oid(self.table_oid)
            .unwrap();
        let mut iterator = self.iterator.lock().unwrap();
        let full_tuple = iterator.next(&mut table_info.table);
        return full_tuple.map(|t| t.1);
    }
}
