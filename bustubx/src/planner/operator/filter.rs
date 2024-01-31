use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalFilterOperator {
    pub predicate: Expr,
}
