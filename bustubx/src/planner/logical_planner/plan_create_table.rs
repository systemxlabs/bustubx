use crate::BustubxResult;

use crate::catalog::Column;
use crate::planner::logical_plan_v2::{CreateTable, LogicalPlanV2};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_table_v2(
        &self,
        name: &sqlparser::ast::ObjectName,
        column_defs: &Vec<sqlparser::ast::ColumnDef>,
    ) -> BustubxResult<LogicalPlanV2> {
        let name = self.plan_table_name(name)?;
        let mut columns = vec![];
        for col_def in column_defs {
            columns.push(Column::new(
                col_def.name.value.clone(),
                (&col_def.data_type).try_into()?,
            ))
        }
        Ok(LogicalPlanV2::CreateTable(CreateTable { name, columns }))
    }
}
