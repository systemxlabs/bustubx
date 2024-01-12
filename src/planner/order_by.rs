use super::expr::Expr;

/// BoundOrderBy is an item in the ORDER BY clause.
#[derive(Debug, Clone)]
pub struct BoundOrderBy {
    pub expression: Expr,
    pub desc: bool,
}
