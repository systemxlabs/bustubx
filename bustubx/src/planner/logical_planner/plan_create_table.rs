use crate::BustubxResult;

use crate::catalog::Column;
use crate::planner::logical_plan::{CreateTable, LogicalPlan};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        column_defs: &Vec<sqlparser::ast::ColumnDef>,
    ) -> BustubxResult<LogicalPlan> {
        let name = self.bind_table_name(name)?;
        let mut columns = vec![];
        for col_def in column_defs {
            columns.push(Column::new(
                col_def.name.value.clone(),
                (&col_def.data_type).try_into()?,
                false,
            ))
        }
        Ok(LogicalPlan::CreateTable(CreateTable { name, columns }))
    }
}
