use crate::execution::execution_plan::ExecutionPlan;
use crate::storage::table_heap::TableIterator;
use crate::{
    execution::ExecutionContext, optimizer::operator::PhysicalOperator, storage::tuple::Tuple,
};
use std::sync::{Arc, Mutex};

use super::VolcanoExecutor;

#[derive(Debug)]
pub struct VolcanoTableScanExecutor {
    pub iterator: Mutex<TableIterator>,
}
impl VolcanoTableScanExecutor {
    pub fn default() -> Self {
        Self {
            iterator: Mutex::new(TableIterator::new(None, None)),
        }
    }
}
impl VolcanoExecutor for VolcanoTableScanExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::TableScan(op) = op.as_ref() {
            println!("init table scan executor");
            let table_info = context.catalog.get_mut_table_by_oid(op.table_oid).unwrap();
            let inited_iterator = table_info.table.iter(None, None);
            let mut iterator = self.iterator.lock().unwrap();
            *iterator = inited_iterator;
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not table scan operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple> {
        if let PhysicalOperator::TableScan(op) = op.as_ref() {
            let table_info = context.catalog.get_mut_table_by_oid(op.table_oid).unwrap();
            let mut iterator = self.iterator.lock().unwrap();
            iterator
                .next(&mut table_info.table)
                .map(|(_meta, tuple)| tuple)
        } else {
            panic!("not table scan operator")
        }
    }
}
