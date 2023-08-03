use std::sync::Arc;

use crate::{
    catalog::column::Column, execution::execution_plan::ExecutionPlan,
    optimizer::operator::PhysicalOperator, storage::tuple::Tuple,
};

use super::VolcanoExecutor;

#[derive(Debug)]
pub struct VolcanoInsertExecutor;
impl VolcanoExecutor for VolcanoInsertExecutor {
    fn init(&mut self) {}
    fn next(&self, op: Arc<PhysicalOperator>, children: Vec<Arc<ExecutionPlan>>) -> Option<Tuple> {
        if let PhysicalOperator::Insert(op) = op.as_ref() {
            let child = children[0].clone();
            let tuple = child.next();
            if tuple.is_some() {
                let tuple = tuple.unwrap();
                // op.insert(tuple);
                // TODO 插入数据库
                println!("insert tuple to database, tuple: {:?}", tuple);
                // TODO 返回次数？
                Some(tuple)
            } else {
                None
            }
        } else {
            panic!("not insert operator")
        }
    }
}
