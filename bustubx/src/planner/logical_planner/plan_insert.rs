use crate::BustubxResult;
use std::sync::Arc;

use crate::planner::logical_plan::{Insert, LogicalPlan};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_insert(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns_ident: &Vec<sqlparser::ast::Ident>,
        source: &sqlparser::ast::Query,
    ) -> BustubxResult<LogicalPlan> {
        let values = self.plan_set_expr(source.body.as_ref())?;
        let table = self.plan_table_name(table_name)?;
        let columns = columns_ident
            .iter()
            .map(|ident| ident.value.clone())
            .collect();
        Ok(LogicalPlan::Insert(Insert {
            table,
            columns,
            input: Arc::new(values),
        }))
    }
}
