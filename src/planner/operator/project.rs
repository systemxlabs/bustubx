use crate::binder::expression::BoundExpression;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalProjectOperator {
    pub expressions: Vec<BoundExpression>,
}
