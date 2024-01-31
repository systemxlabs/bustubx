use crate::common::ScalarValue;
use crate::expression::{Alias, Expr, Literal};
use sqlparser::ast::{Offset, OrderByExpr, Query, SelectItem, SetExpr};
use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;
use crate::planner::order_by::BoundOrderBy;

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_select(&mut self, query: &Query) -> LogicalPlan {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => &**select,
            _ => unimplemented!(),
        };

        let from_table = self.bind_from(&select.from);

        // bind select list
        let mut select_list = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.plan_expr(expr).unwrap();
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.plan_expr(expr).unwrap();
                    select_list.push(Expr::Alias(Alias {
                        name: alias.value.clone(),
                        expr: Box::new(expr),
                    }));
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    // TODO
                    // let qualifier = format!("{}", object_name);
                    // select_list.extend_from_slice(
                    // self.bind_qualified_columns_in_context(qualifier).as_slice(),
                    // )
                }
                SelectItem::Wildcard(_) => {
                    select_list.extend(from_table.gen_select_list());
                }
            }
        }

        // bind where clause
        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.plan_expr(expr).unwrap());

        // bind limit and offset
        let (limit, offset) = self.bind_limit(&query.limit, &query.offset);

        // bind order by clause
        let sort = self.bind_order_by(&query.order_by);

        // from table
        let mut plan = self.plan_table_ref(from_table);

        // filter
        if where_clause.is_some() {
            let mut filter_plan = LogicalPlan {
                operator: LogicalOperator::new_filter_operator(where_clause.unwrap()),
                children: Vec::new(),
            };
            filter_plan.children.push(Arc::new(plan));
            plan = filter_plan;
        }

        // project
        let mut plan = LogicalPlan {
            operator: LogicalOperator::new_project_operator(select_list),
            children: vec![Arc::new(plan)],
        };

        // order by clause may use computed column, so it should be after project
        // for example, `select a+b from t order by a+b limit 10`
        if !sort.is_empty() {
            let mut sort_plan = LogicalPlan {
                operator: LogicalOperator::new_sort_operator(sort),
                children: Vec::new(),
            };
            sort_plan.children.push(Arc::new(plan));
            plan = sort_plan;
        }

        // limit
        if limit.is_some() || offset.is_some() {
            let mut limit_plan = self.plan_limit(&limit, &offset);
            limit_plan.children.push(Arc::new(plan));
            plan = limit_plan;
        }

        plan
    }

    pub fn plan_limit(&self, limit: &Option<Expr>, offset: &Option<Expr>) -> LogicalPlan {
        let limit = limit.as_ref().map(|limit| match limit {
            Expr::Literal(ref lit) => match lit.value {
                ScalarValue::Int8(Some(v)) => v as usize,
                ScalarValue::Int16(Some(v)) => v as usize,
                ScalarValue::Int32(Some(v)) => v as usize,
                ScalarValue::Int64(Some(v)) => v as usize,
                ScalarValue::UInt64(Some(v)) => v as usize,
                _ => panic!("limit must be a number"),
            },
            _ => panic!("limit must be a number"),
        });
        let offset = offset.as_ref().map(|offset| match offset {
            Expr::Literal(ref lit) => match lit.value {
                ScalarValue::Int8(Some(v)) => v as usize,
                ScalarValue::Int16(Some(v)) => v as usize,
                ScalarValue::Int32(Some(v)) => v as usize,
                ScalarValue::Int64(Some(v)) => v as usize,
                ScalarValue::UInt64(Some(v)) => v as usize,
                _ => panic!("limit must be a number"),
            },
            _ => panic!("offset must be a number"),
        });
        LogicalPlan {
            operator: LogicalOperator::new_limit_operator(limit, offset),
            children: Vec::new(),
        }
    }

    pub fn bind_limit(
        &self,
        limit: &Option<sqlparser::ast::Expr>,
        offset: &Option<Offset>,
    ) -> (Option<Expr>, Option<Expr>) {
        let limit = limit.as_ref().map(|expr| self.plan_expr(&expr).unwrap());
        let offset = offset
            .as_ref()
            .map(|offset| self.plan_expr(&offset.value).unwrap());
        (limit, offset)
    }

    pub fn bind_order_by(&self, order_by_list: &Vec<OrderByExpr>) -> Vec<BoundOrderBy> {
        order_by_list
            .iter()
            .map(|expr| BoundOrderBy {
                expression: self.plan_expr(&expr.expr).unwrap(),
                desc: expr.asc.map_or(false, |asc| !asc),
            })
            .collect::<Vec<BoundOrderBy>>()
    }
}
