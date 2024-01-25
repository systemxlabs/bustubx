use crate::planner::expr::column_ref::ColumnRef;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;
use sqlparser::ast::{ObjectName, OrderByExpr};

use super::Planner;

impl<'a> Planner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &ObjectName,
        table_name: &ObjectName,
        columns: &Vec<OrderByExpr>,
    ) -> LogicalPlan {
        let table = self.bind_base_table_by_name(table_name.to_string().as_str(), None);
        let columns = columns
            .iter()
            .map(|column| self.bind_column_ref_expr(&column.expr))
            .collect::<Vec<ColumnRef>>();
        let table_schema = table.schema;
        let mut key_attrs = Vec::new();
        for col in columns {
            let index = table_schema
                .get_index_by_name(&col.col_name)
                .expect("col not found");
            key_attrs.push(index as u32);
        }
        LogicalPlan {
            operator: LogicalOperator::new_create_index_operator(
                index_name.to_string(),
                table.table,
                table_schema,
                key_attrs,
            ),
            children: Vec::new(),
        }
    }
}
