use crate::BustubxResult;

use crate::catalog::Column;
use crate::catalog::Schema;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::logical_plan_v2::{CreateTable, LogicalPlanV2};
use crate::planner::operator::LogicalOperator;

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        column_defs: &Vec<sqlparser::ast::ColumnDef>,
    ) -> LogicalPlan {
        let table_name = name.to_string();
        let columns = column_defs
            .iter()
            .map(|c| Column::from_sqlparser_column(c))
            .collect();
        let schema = Schema::new(columns);
        LogicalPlan {
            operator: LogicalOperator::new_create_table_operator(table_name, schema),
            children: Vec::new(),
        }
    }

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
