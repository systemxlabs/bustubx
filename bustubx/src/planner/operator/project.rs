use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalProjectOperator {
    pub expressions: Vec<Expr>,
}
