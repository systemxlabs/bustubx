use std::sync::Arc;

use crate::binder::statement::insert::InsertStatement;

use super::{logical_plan::LogicalPlan, operator::LogicalOperator, Planner};

impl Planner {
    pub fn plan_insert(&self, stmt: InsertStatement) -> LogicalPlan {
        let values_node = LogicalPlan {
            operator: LogicalOperator::new_values_operator(stmt.columns.clone(), stmt.values),
            children: Vec::new(),
        };
        LogicalPlan {
            operator: LogicalOperator::new_insert_operator(stmt.table.table, stmt.columns),
            children: vec![Arc::new(values_node)],
        }
    }
}
