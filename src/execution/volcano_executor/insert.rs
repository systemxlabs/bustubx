use std::sync::{Arc, Mutex};

use crate::{
    catalog::column::Column,
    dbtype::value::Value,
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::operator::PhysicalOperator,
    storage::{
        table_heap,
        tuple::{Tuple, TupleMeta},
    },
};

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoInsertExecutor {
    pub insert_rows: Mutex<u32>,
}
impl VolcanoInsertExecutor {
    pub fn new() -> Self {
        Self {
            insert_rows: Mutex::new(0),
        }
    }
}
impl VolcanoExecutor for VolcanoInsertExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::Insert(op) = op.as_ref() {
            println!("init insert executor");
            *self.insert_rows.lock().unwrap() = 0;
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
            let mut insert_rows = self.insert_rows.lock().unwrap();
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
                *insert_rows += 1;
            }
            // 只在最后一次next的时候返回插入的行数
            if next_result.exhausted {
                NextResult::new(
                    Some(Tuple::from_values(vec![Value::Integer(
                        *insert_rows as i32,
                    )])),
                    next_result.exhausted,
                )
            } else {
                NextResult::new(None, next_result.exhausted)
            }
        } else {
            panic!("not insert operator")
        }
    }
}
