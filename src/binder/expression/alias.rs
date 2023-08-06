use crate::{catalog::schema::Schema, dbtype::value::Value, storage::tuple::Tuple};

use super::BoundExpression;

/// The alias in SELECT list, e.g. `SELECT count(x) AS y`, the `y` is an alias.
#[derive(Debug, Clone)]
pub struct BoundAlias {
    pub alias: String,
    pub child: Box<BoundExpression>,
}
impl BoundAlias {
    pub fn evaluate(&self, tuple: Option<&Tuple>, schema: Option<&Schema>) -> Value {
        self.child.evaluate(tuple, schema)
    }
}
