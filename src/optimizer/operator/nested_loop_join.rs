use std::sync::Arc;

use crate::{
    binder::{expression::BoundExpression, table_ref::join::JoinType},
    catalog::schema::Schema,
};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalNestedLoopJoinOperator {
    pub join_type: JoinType,
    pub condition: Option<BoundExpression>,
    pub left_input: Arc<PhysicalOperator>,
    pub right_input: Arc<PhysicalOperator>,
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
        Schema::from_schemas(vec![
            self.left_input.output_schema(),
            self.right_input.output_schema(),
        ])
    }
}
