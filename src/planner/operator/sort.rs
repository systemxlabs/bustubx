use crate::binder::expression::BoundExpression;

#[derive(Debug, Clone)]
pub struct LogicalSortOperator {
    pub expr: BoundExpression,
    pub desc: bool,
}
impl LogicalSortOperator {
    pub fn new(expr: BoundExpression, desc: bool) -> Self {
        Self { expr, desc }
    }
}
