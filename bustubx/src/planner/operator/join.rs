use crate::planner::{expr::Expr, table_ref::join::JoinType};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalJoinOperator {
    pub join_type: JoinType,
    pub condition: Option<Expr>,
}
