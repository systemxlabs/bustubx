use crate::expression::Expr;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::logical_plan_v2::{CreateIndex, LogicalPlanV2};
use crate::planner::operator::LogicalOperator;
use crate::{BustubxError, BustubxResult};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &sqlparser::ast::ObjectName,
        table_name: &sqlparser::ast::ObjectName,
        columns: &Vec<sqlparser::ast::OrderByExpr>,
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

    pub fn plan_create_index_v2(
        &self,
        index_name: &sqlparser::ast::ObjectName,
        table_name: &sqlparser::ast::ObjectName,
        columns: &Vec<sqlparser::ast::OrderByExpr>,
    ) -> BustubxResult<LogicalPlanV2> {
        let index_name = index_name
            .0
            .get(0)
            .map_or(Err(BustubxError::Plan("".to_string())), |ident| {
                Ok(ident.value.clone())
            })?;
        let table = self.plan_table_name(table_name)?;
        let mut columns_expr = vec![];
        for col in columns.iter() {
            let col_expr = self.plan_order_by_v2(&col)?;
            columns_expr.push(col_expr);
        }
        Ok(LogicalPlanV2::CreateIndex(CreateIndex {
            index_name,
            table,
            columns: columns_expr,
        }))
    }
}
