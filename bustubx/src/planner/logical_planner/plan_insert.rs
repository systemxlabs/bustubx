use crate::expression::{Cast, Expr};
use crate::BustubxResult;
use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;
use crate::planner::logical_plan_v2::{Insert, LogicalPlanV2};
use crate::planner::operator::LogicalOperator;

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_insert(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns_ident: &Vec<sqlparser::ast::Ident>,
        source: &sqlparser::ast::Query,
    ) -> LogicalPlan {
        if let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() {
            if let Some(table_info) = self
                .context
                .catalog
                .get_table_by_name(&table_name.to_string())
            {
                let mut columns = Vec::new();
                if columns_ident.is_empty() {
                    columns = table_info.schema.columns.clone();
                } else {
                    for column_ident in columns_ident {
                        if let Some(column) = table_info.schema.get_col_by_name(&column_ident.value)
                        {
                            columns.push(column.clone());
                        } else {
                            panic!(
                                "Column {} not found in table {}",
                                column_ident.value, table_name
                            );
                        }
                    }
                }

                let mut records = Vec::new();
                for row in values.rows.iter() {
                    let mut record = Vec::new();
                    for (idx, expr) in row.iter().enumerate() {
                        record.push(Expr::Cast(Cast {
                            data_type: columns[idx].data_type,
                            expr: Box::new(self.plan_expr(expr).unwrap()),
                        }))
                    }
                    records.push(record);
                }
                let values_node = LogicalPlan {
                    operator: LogicalOperator::new_values_operator(columns.clone(), records),
                    children: Vec::new(),
                };
                LogicalPlan {
                    operator: LogicalOperator::new_insert_operator(
                        table_info.name.clone(),
                        columns,
                    ),
                    children: vec![Arc::new(values_node)],
                }
            } else {
                panic!("Table {} not found", table_name);
            }
        } else {
            unimplemented!()
        }
    }

    pub fn plan_insert_v2(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns_ident: &Vec<sqlparser::ast::Ident>,
        source: &sqlparser::ast::Query,
    ) -> BustubxResult<LogicalPlanV2> {
        let values = self.plan_set_expr(source.body.as_ref())?;
        let table = self.plan_table_name(table_name)?;
        let columns = columns_ident
            .iter()
            .map(|ident| ident.value.clone())
            .collect();
        Ok(LogicalPlanV2::Insert(Insert {
            table,
            columns,
            input: Arc::new(values),
        }))
    }
}
