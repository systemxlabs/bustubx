use crate::{catalog::schema::Schema, planner::expr::Expr};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalFilterOperator {
    pub predicate: Expr,
}
