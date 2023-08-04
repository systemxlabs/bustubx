use super::BoundExpression;

/// The alias in SELECT list, e.g. `SELECT count(x) AS y`, the `y` is an alias.
#[derive(Debug, Clone)]
pub struct BoundAlias {
    pub alias: String,
    pub child: Box<BoundExpression>,
}
