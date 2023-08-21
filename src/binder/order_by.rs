use super::expression::BoundExpression;

/// BoundOrderBy is an item in the ORDER BY clause.
#[derive(Debug, Clone)]
pub struct BoundOrderBy {
    pub expression: BoundExpression,
    pub desc: bool,
}
