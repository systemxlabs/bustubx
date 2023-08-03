use crate::{
    catalog::column::Column,
    execution::{execution_plan::ExecutionPlan, ExecutionContext},
    optimizer::operator::PhysicalOperator,
    storage::{
        table_heap,
        tuple::{Tuple, TupleMeta},
    },
};
use std::sync::Arc;

use super::VolcanoExecutor;

#[derive(Debug)]
pub struct VolcanoCreateTableExecutor;
impl VolcanoExecutor for VolcanoCreateTableExecutor {
    fn init(&mut self) {}
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> Option<Tuple> {
        if let PhysicalOperator::CreateTable(op) = op.as_ref() {
            let table_name = op.table_name.clone();
            let schema = op.schema.clone();
            context.catalog.create_table(table_name, schema);
            println!("create table: {:?}, schema: {:?}", op.table_name, op.schema);
            None
        } else {
            panic!("not create table operator")
        }
    }
}
