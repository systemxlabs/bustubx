use std::sync::Arc;

use super::operator::LogicalOperator;

#[derive(Debug)]
pub struct LogicalPlan {
    pub operator: LogicalOperator,
    pub children: Vec<Arc<LogicalPlan>>,
}
