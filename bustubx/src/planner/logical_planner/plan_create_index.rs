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
        let index_name = index_name.0.get(0).map_or(
            Err(BustubxError::Plan(format!(
                "Index name {index_name} is not expected"
            ))),
            |ident| Ok(ident.value.clone()),
        )?;
        let table = self.bind_table_name(table_name)?;
        let mut columns_expr = vec![];
        for col in columns.iter() {
            let col_expr = self.bind_order_by_expr(&col)?;
            columns_expr.push(col_expr);
        }
        let table_schema = self
            .context
            .catalog
            .get_table_by_name(table.table())?
            .schema
            .clone();
        Ok(LogicalPlan::CreateIndex(CreateIndex {
            index_name,
            table,
            table_schema,
            columns: columns_expr,
        }))
    }
}
