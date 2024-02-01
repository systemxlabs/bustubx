use crate::BustubxResult;
use std::sync::Arc;

use crate::planner::logical_plan_v2::{Insert, LogicalPlanV2};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_insert_v2(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns_ident: &Vec<sqlparser::ast::Ident>,
        source: &sqlparser::ast::Query,
    ) -> BustubxResult<LogicalPlanV2> {
        let values = self.plan_set_expr(source.body.as_ref())?;
        let table = self.plan_table_name(table_name)?;
        let columns = columns_ident
            .iter()
            .map(|ident| ident.value.clone())
            .collect();
        Ok(LogicalPlanV2::Insert(Insert {
            table,
            columns,
            input: Arc::new(values),
        }))
    }
}
