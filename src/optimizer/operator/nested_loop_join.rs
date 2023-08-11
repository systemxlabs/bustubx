use std::sync::Arc;

use crate::{
    binder::{expression::BoundExpression, table_ref::join::JoinType},
    catalog::schema::Schema,
};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalNestedLoopJoinOperator {
    join_type: JoinType,
    condition: Option<BoundExpression>,
    left_input: Arc<PhysicalOperator>,
    right_input: Arc<PhysicalOperator>,
}
impl PhysicalNestedLoopJoinOperator {
    pub fn new(
        join_type: JoinType,
        condition: Option<BoundExpression>,
        left_input: Arc<PhysicalOperator>,
        right_input: Arc<PhysicalOperator>,
    ) -> Self {
        PhysicalNestedLoopJoinOperator {
            join_type,
            condition,
            left_input,
            right_input,
        }
    }
    pub fn output_schema(&self) -> Schema {
        let mut columns = self.left_input.output_schema().columns;
        columns.append(&mut self.right_input.output_schema().columns);
        Schema::new(columns)
    }
}
