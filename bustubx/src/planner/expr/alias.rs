use crate::{catalog::Schema, common::ScalarValue, storage::Tuple};

use super::Expr;

/// The alias in SELECT list, e.g. `SELECT count(x) AS y`, the `y` is an alias.
#[derive(Debug, Clone)]
pub struct Alias {
    pub alias: String,
    pub expr: Box<Expr>,
}
impl Alias {
    pub fn evaluate(&self, tuple: Option<&Tuple>) -> ScalarValue {
        self.expr.evaluate(tuple)
    }
}
