use crate::{
    binder::expression::{column_ref::BoundColumnRef, BoundExpression},
    catalog::schema::Schema,
};

#[derive(Debug, Clone)]
pub struct LogicalLimitOperator {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
impl LogicalLimitOperator {
    pub fn new(limit: Option<usize>, offset: Option<usize>) -> Self {
        Self { limit, offset }
    }
}
