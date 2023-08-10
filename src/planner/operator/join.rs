use crate::{
    binder::{
        expression::{column_ref::BoundColumnRef, BoundExpression},
        table_ref::join::JoinType,
    },
    catalog::schema::Schema,
};

#[derive(Debug)]
pub struct LogicalJoinOperator {
    pub join_type: JoinType,
    pub condition: Option<BoundExpression>,
}
impl LogicalJoinOperator {
    pub fn new(join_type: JoinType, condition: Option<BoundExpression>) -> Self {
        Self {
            join_type,
            condition,
        }
    }
}
