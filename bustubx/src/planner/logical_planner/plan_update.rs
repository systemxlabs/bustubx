use crate::planner::logical_plan::{LogicalPlan, Update};
use crate::planner::LogicalPlanner;
use crate::{BustubxError, BustubxResult};
use std::collections::HashMap;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_update(
        &self,
        table: &sqlparser::ast::TableWithJoins,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<sqlparser::ast::Expr>,
    ) -> BustubxResult<LogicalPlan> {
        let table_ref = match &table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.bind_table_name(name)?,
            _ => {
                return Err(BustubxError::Plan(format!(
                    "table {} is not supported",
                    table
                )))
            }
        };

        let table_schema = self.context.catalog.table_heap(&table_ref)?.schema.clone();

        let mut assignment_map = HashMap::new();
        for assign in assignments {
            let column_name = assign
                .id
                .get(0)
                .ok_or(BustubxError::Plan(format!(
                    "Assignment {} is not supported",
                    assign
                )))?
                .value
                .clone();
            let value = self.bind_expr(&assign.value)?;
            assignment_map.insert(column_name, value);
        }

        let selection = match selection {
            Some(e) => Some(self.bind_expr(e)?),
            None => None,
        };

        Ok(LogicalPlan::Update(Update {
            table: table_ref,
            table_schema,
            assignments: assignment_map,
            selection,
        }))
    }
}
