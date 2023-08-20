use super::expression::BoundExpression;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrderByType {
    #[default]
    ASC,
    DESC,
}

/// BoundOrderBy is an item in the ORDER BY clause.
#[derive(Debug, Clone)]
pub struct BoundOrderBy {
    pub order_by_type: OrderByType,
    pub expression: BoundExpression,
}
