use crate::planner::expr::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalProjectOperator {
    pub expressions: Vec<Expr>,
}
