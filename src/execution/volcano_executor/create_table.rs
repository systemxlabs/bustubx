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

use super::{NextResult, VolcanoExecutor};

#[derive(Debug)]
pub struct VolcanoCreateTableExecutor;
impl VolcanoExecutor for VolcanoCreateTableExecutor {
    fn init(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) {
        if let PhysicalOperator::CreateTable(op) = op.as_ref() {
            println!("init create table executor");
            for child in children {
                child.init(context);
            }
        } else {
            panic!("not create table operator")
        }
    }
    fn next(
        &self,
        context: &mut ExecutionContext,
        op: Arc<PhysicalOperator>,
        children: Vec<Arc<ExecutionPlan>>,
    ) -> NextResult {
        if let PhysicalOperator::CreateTable(op) = op.as_ref() {
            let table_name = op.table_name.clone();
            let schema = op.schema.clone();
            context.catalog.create_table(table_name, schema);
            NextResult::new(None, true)
        } else {
            panic!("not create table operator")
        }
    }
}
