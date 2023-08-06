use std::sync::Arc;

use crate::{
    catalog::column::Column,
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::operator::PhysicalOperator,
    storage::{
        table_heap,
        tuple::{Tuple, TupleMeta},
    },
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoInsertExecutor;
impl VolcanoExecutor for VolcanoInsertExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::Insert(op) = op.as_ref() {
            println!("init insert executor");
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not insert operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalOperator::Insert(op) = op.as_ref() {
            let child = children[0].clone();
            let next_result = child.next(context);
            if next_result.tuple.is_some() {
                let tuple = next_result.tuple.unwrap();
                // 插入数据库
                let table_heap = &mut context
                    .catalog
                    .get_mut_table_by_name(&op.table_name)
                    .unwrap()
                    .table;
                let tuple_meta = TupleMeta {
                    insert_txn_id: 0,
                    delete_txn_id: 0,
                    is_deleted: false,
                };
                table_heap.insert_tuple(&tuple_meta, &tuple);
                println!("insert tuple to database, tuple: {:?}", tuple);
                NextResult::new(Some(tuple), next_result.exhusted)
            } else {
                NextResult::new(None, next_result.exhusted)
            }
        } else {
            panic!("not insert operator")
        }
    }
}
