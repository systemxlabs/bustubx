use std::sync::Arc;

use crate::{
    binder::statement::BoundStatement,
    planner::operator::{insert::InsertOperator, values::ValuesOperator},
};

use self::operator::LogicalOperator;

pub mod operator;

#[derive(Debug)]
pub struct LogicalPlan {
    pub operator: LogicalOperator,
    pub children: Vec<Arc<LogicalPlan>>,
}

pub struct Planner {}
impl Planner {
    // 根据BoundStatement生成逻辑计划
    pub fn plan(&mut self, statement: BoundStatement) -> LogicalPlan {
        match statement {
            BoundStatement::Insert(stmt) => {
                let values_node = LogicalPlan {
                    operator: LogicalOperator::new_values_operator(stmt.columns, stmt.values),
                    children: Vec::new(),
                };
                LogicalPlan {
                    operator: LogicalOperator::new_insert_operator(stmt.table.table),
                    children: vec![Arc::new(values_node)],
                }
            }
            _ => unimplemented!(),
        }
    }
}
