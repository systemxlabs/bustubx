use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalFilterOperator {
    pub predicate: BoundExpression,
}
