use crate::execution::execution_plan::ExecutionPlan;
use crate::storage::table_heap::TableIterator;
use crate::{
    execution::ExecutionContext, optimizer::operator::PhysicalPlanV2, storage::tuple::Tuple,
};
use std::sync::{Arc, Mutex};

use super::{NextResult, VolcanoExecutor};

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
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalPlanV2::TableScan(op) = op.as_ref() {
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
        op: Arc<PhysicalPlanV2>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalPlanV2::TableScan(op) = op.as_ref() {
            let table_info = context.catalog.get_mut_table_by_oid(op.table_oid).unwrap();
            let mut iterator = self.iterator.lock().unwrap();
            let tuple = iterator.next(&mut table_info.table);
            if tuple.is_none() {
                return NextResult::new(None, true);
            } else {
                return NextResult::new(Some(tuple.unwrap().1), false);
            }
        } else {
            panic!("not table scan operator")
        }
    }
}
