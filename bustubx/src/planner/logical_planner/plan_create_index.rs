use crate::planner::logical_plan::{CreateIndex, LogicalPlan};
use crate::{BustubxError, BustubxResult};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &sqlparser::ast::ObjectName,
        table_name: &sqlparser::ast::ObjectName,
        columns: &Vec<sqlparser::ast::OrderByExpr>,
    ) -> BustubxResult<LogicalPlan> {
        let index_name = index_name
            .0
            .get(0)
            .map_or(Err(BustubxError::Plan("".to_string())), |ident| {
                Ok(ident.value.clone())
            })?;
        let table = self.plan_table_name(table_name)?;
        let mut columns_expr = vec![];
        for col in columns.iter() {
            let col_expr = self.plan_order_by_expr(&col)?;
            columns_expr.push(col_expr);
        }
        let table_schema = self
            .context
            .catalog
            .get_table_by_name(table.table())
            .map_or(
                Err(BustubxError::Plan(format!("table {} not found", table))),
                |info| Ok(info.schema.clone()),
            )?;
        Ok(LogicalPlan::CreateIndex(CreateIndex {
            index_name,
            table,
            table_schema,
            columns: columns_expr,
        }))
    }
}
