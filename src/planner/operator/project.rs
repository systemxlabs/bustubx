use crate::{
    binder::expression::{column_ref::BoundColumnRef, BoundExpression},
    catalog::schema::Schema,
};

#[derive(Debug, Clone)]
pub struct LogicalProjectOperator {
    pub expressions: Vec<BoundExpression>,
}
impl LogicalProjectOperator {
    pub fn new(expressions: Vec<BoundExpression>) -> Self {
        Self { expressions }
    }
}
