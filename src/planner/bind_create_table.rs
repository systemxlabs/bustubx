use sqlparser::ast::{ColumnDef, ObjectName};

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;

use super::Planner;

impl<'a> Planner<'a> {
    pub fn plan_create_table(
        &self,
        name: &ObjectName,
        column_defs: &Vec<ColumnDef>,
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
}
