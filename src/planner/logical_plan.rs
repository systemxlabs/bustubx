use std::sync::Arc;

use crate::catalog::schema::Schema;

use super::operator::LogicalOperator;

#[derive(Debug)]
pub struct LogicalPlan {
    pub operator: LogicalOperator,
    pub children: Vec<Arc<LogicalPlan>>,
}
impl LogicalPlan {
    pub fn output_schema(&self) -> Schema {
        self.operator.output_schema()
    }
}
