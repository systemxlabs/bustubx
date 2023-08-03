use crate::binder::expression::BoundExpression;

use super::BoundTableRef;

#[derive(Debug)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Outer,
}

/// A join. e.g., `SELECT * FROM x INNER JOIN y ON ...`, where `x INNER JOIN y ON ...` is `BoundJoinRef`.
#[derive(Debug)]
pub struct BoundJoinRef {
    pub join_type: JoinType,
    pub left: Box<BoundTableRef>,
    pub right: Box<BoundTableRef>,
    pub condition: BoundExpression,
}
