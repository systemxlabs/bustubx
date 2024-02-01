use crate::common::ScalarValue;
use crate::expression::Expr;
use crate::{BustubxError, BustubxResult};
use std::sync::Arc;

use crate::planner::logical_plan_v2::{Limit, LogicalPlanV2, Sort};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_query_v2(&self, query: &sqlparser::ast::Query) -> BustubxResult<LogicalPlanV2> {
        let plan = self.plan_set_expr(&query.body)?;
        let plan = self.plan_order_by(plan, &query.order_by)?;
        self.plan_limit_v2(plan, &query.limit, &query.offset)
    }

    pub fn plan_order_by(
        &self,
        input: LogicalPlanV2,
        order_by: &Vec<sqlparser::ast::OrderByExpr>,
    ) -> BustubxResult<LogicalPlanV2> {
        if order_by.is_empty() {
            return Ok(input);
        }

        let mut order_by_exprs = vec![];
        for order in order_by {
            order_by_exprs.push(self.plan_order_by_expr(order)?);
        }

        Ok(LogicalPlanV2::Sort(Sort {
            expr: order_by_exprs,
            input: Arc::new(input),
            limit: None,
        }))
    }

    pub fn plan_limit_v2(
        &self,
        input: LogicalPlanV2,
        limit: &Option<sqlparser::ast::Expr>,
        offset: &Option<sqlparser::ast::Offset>,
    ) -> BustubxResult<LogicalPlanV2> {
        if limit.is_none() && offset.is_none() {
            return Ok(input);
        }

        let limit = match limit {
            None => None,
            Some(limit_expr) => {
                let n = match self.plan_expr(&limit_expr)? {
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
            Some(offset_expr) => match self.plan_expr(&offset_expr.value)? {
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

        Ok(LogicalPlanV2::Limit(Limit {
            limit,
            offset,
            input: Arc::new(input),
        }))
    }
}
