use crate::common::ScalarValue;
use crate::expression::Expr;
use crate::{BustubxError, BustubxResult};
use std::sync::Arc;

use crate::planner::logical_plan::{Limit, LogicalPlan, Sort};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_query(&self, query: &sqlparser::ast::Query) -> BustubxResult<LogicalPlan> {
        let plan = self.plan_set_expr(&query.body)?;
        let plan = self.plan_order_by(plan, &query.order_by)?;
        self.plan_limit(plan, &query.limit, &query.offset)
    }

    pub fn plan_order_by(
        &self,
        input: LogicalPlan,
        order_by: &Vec<sqlparser::ast::OrderByExpr>,
    ) -> BustubxResult<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(input);
        }

        let mut order_by_exprs = vec![];
        for order in order_by {
            order_by_exprs.push(self.bind_order_by_expr(order)?);
        }

        Ok(LogicalPlan::Sort(Sort {
            expr: order_by_exprs,
            input: Arc::new(input),
            limit: None,
        }))
    }

    pub fn plan_limit(
        &self,
        input: LogicalPlan,
        limit: &Option<sqlparser::ast::Expr>,
        offset: &Option<sqlparser::ast::Offset>,
    ) -> BustubxResult<LogicalPlan> {
        if limit.is_none() && offset.is_none() {
            return Ok(input);
        }

        let limit = match limit {
            None => None,
            Some(limit_expr) => {
                let n = match self.bind_expr(&limit_expr)? {
                    Expr::Literal(lit) => match lit.value {
                        ScalarValue::Int64(Some(v)) if v >= 0 => Ok(v as usize),
                        _ => Err(BustubxError::Plan(format!(
                            "LIMIT must not be negative, {}",
                            lit.value
                        ))),
                    },
                    _ => Err(BustubxError::Plan(format!(
                        "LIMIT must be literal, {}",
                        limit_expr
                    ))),
                }?;
                Some(n)
            }
        };

        let offset = match offset {
            None => 0,
            Some(offset_expr) => match self.bind_expr(&offset_expr.value)? {
                Expr::Literal(lit) => match lit.value {
                    ScalarValue::Int64(Some(v)) => {
                        if v < 0 {
                            return Err(BustubxError::Plan(format!("Offset must be >= 0, {}", v)));
                        }
                        Ok(v as usize)
                    }
                    _ => Err(BustubxError::Plan(format!(
                        "Offset value not int64, {}",
                        lit.value
                    ))),
                },
                _ => Err(BustubxError::Plan(format!(
                    "Offset expression not expected, {}",
                    offset_expr
                ))),
            }?,
        };

        Ok(LogicalPlan::Limit(Limit {
            limit,
            offset,
            input: Arc::new(input),
        }))
    }
}
