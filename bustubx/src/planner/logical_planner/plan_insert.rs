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
        let table = self.bind_table_name(table_name)?;
        let table_schema = self.context.catalog.table_heap(&table)?.schema.clone();

        let projected_schema = if columns_ident.is_empty() {
            table_schema.clone()
        } else {
            let columns: Vec<String> = columns_ident
                .iter()
                .map(|ident| ident.value.clone())
                .collect();
            let indices = columns
                .iter()
                .map(|name| table_schema.index_of(Some(&table), name.as_str()))
                .collect::<BustubxResult<Vec<usize>>>()?;

            table_schema.project(&indices)?
        };

        Ok(LogicalPlan::Insert(Insert {
            table,
            table_schema,
            projected_schema,
            input: Arc::new(values),
        }))
    }
}
