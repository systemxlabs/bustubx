use crate::binder::{expression::BoundExpression, table_ref::join::JoinType};

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
