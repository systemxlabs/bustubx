use crate::catalog::ColumnRef;
use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalValuesOperator {
    pub columns: Vec<ColumnRef>,
    pub tuples: Vec<Vec<Expr>>,
}
