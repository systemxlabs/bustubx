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

use super::VolcanoExecutor;

#[derive(Debug)]
pub struct VolcanoInsertExecutor;
impl VolcanoExecutor for VolcanoInsertExecutor {
    fn init(&mut self) {}
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple> {
        if let PhysicalOperator::Insert(op) = op.as_ref() {
            let child = children[0].clone();
            let tuple = child.next(context);
            if tuple.is_some() {
                let tuple = tuple.unwrap();
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
                Some(tuple)
            } else {
                None
            }
        } else {
            panic!("not insert operator")
        }
    }
}
