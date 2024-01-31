use crate::catalog::{Column, Schema};
use crate::expression::ExprTrait;
use crate::planner::logical_plan_v2::{LogicalPlanV2, Values};
use crate::planner::LogicalPlanner;
use crate::{BustubxError, BustubxResult};
use std::sync::Arc;

impl LogicalPlanner<'_> {
    pub fn plan_set_expr(
        &self,
        set_expr: &sqlparser::ast::SetExpr,
    ) -> BustubxResult<LogicalPlanV2> {
        match set_expr {
            sqlparser::ast::SetExpr::Select(select) => todo!(),
            sqlparser::ast::SetExpr::Query(_) => todo!(),
            sqlparser::ast::SetExpr::Values(values) => self.plan_values(values),
            _ => Err(BustubxError::Plan(format!(
                "Failed to plan set expr: {}",
                set_expr
            ))),
        }
    }

    pub fn plan_values(&self, values: &sqlparser::ast::Values) -> BustubxResult<LogicalPlanV2> {
        let mut result = vec![];
        for row in values.rows.iter() {
            let mut record = vec![];
            for item in row {
                record.push(self.plan_expr(item)?);
            }
            result.push(record);
        }
        if result.is_empty() {
            return Ok(LogicalPlanV2::Values(Values {
                schema: Arc::new(Schema::empty()),
                values: vec![],
            }));
        }

        // parse schema
        let first_row = &result[0];
        let mut columns = vec![];
        for (idx, item) in first_row.iter().enumerate() {
            columns.push(Column::new(
                idx.to_string(),
                item.data_type(&Schema::empty())?,
            ))
        }

        Ok(LogicalPlanV2::Values(Values {
            schema: Arc::new(Schema::new(columns)),
            values: result,
        }))
    }
}
