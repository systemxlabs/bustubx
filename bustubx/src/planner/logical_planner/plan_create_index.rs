use crate::expression::{ColumnExpr, Expr};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;
use sqlparser::ast::{ObjectName, OrderByExpr};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &ObjectName,
        table_name: &ObjectName,
        columns: &Vec<OrderByExpr>,
    ) -> LogicalPlan {
        let table = self.bind_base_table_by_name(table_name.to_string().as_str(), None);
        let columns = columns
            .iter()
            .map(|column| self.plan_expr(&column.expr).unwrap())
            .collect::<Vec<Expr>>();
        let table_schema = table.schema;
        let mut key_attrs = Vec::new();
        for col in columns {
            if let Expr::Column(col) = col {
                let index = table_schema
                    .get_index_by_name(&col.name)
                    .expect("col not found");
                key_attrs.push(index as u32);
            }
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
