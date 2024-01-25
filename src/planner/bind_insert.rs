use sqlparser::ast::{Ident, ObjectName, Query, SetExpr};
use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;

use super::{expr::Expr, Planner};

impl<'a> Planner<'a> {
    pub fn plan_insert(
        &self,
        table_name: &ObjectName,
        columns_ident: &Vec<Ident>,
        source: &Query,
    ) -> LogicalPlan {
        if let SetExpr::Values(values) = source.body.as_ref() {
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
                    for expr in row {
                        let data_type = columns[record.len()].data_type;
                        if let Expr::Constant(constant) = self.bind_expression(expr) {
                            record.push(constant.value.to_value(data_type));
                        }
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
}
