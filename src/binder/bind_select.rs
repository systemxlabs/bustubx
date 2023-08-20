use sqlparser::ast::{Expr, Offset, OrderByExpr, Query, SelectItem, SetExpr};

use crate::binder::expression::{alias::BoundAlias, BoundExpression};

use super::{order_by::BoundOrderBy, statement::select::SelectStatement, Binder};

impl<'a> Binder<'a> {
    pub fn bind_select(&self, query: &Query) -> SelectStatement {
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
                    let expr = self.bind_expression(expr);
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expression(expr);
                    select_list.push(BoundExpression::Alias(BoundAlias {
                        alias: alias.value.clone(),
                        child: Box::new(expr),
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
            .map(|expr| self.bind_expression(expr));

        // bind limit and offset
        let (limit, offset) = self.bind_limit(&query.limit, &query.offset);

        // bind order by clause
        let sort = self.bind_order_by(&query.order_by);

        SelectStatement {
            select_list,
            from_table,
            where_clause,
            limit,
            offset,
            sort,
        }
    }

    pub fn bind_limit(
        &self,
        limit: &Option<Expr>,
        offset: &Option<Offset>,
    ) -> (Option<BoundExpression>, Option<BoundExpression>) {
        let limit = limit.as_ref().map(|expr| self.bind_expression(&expr));
        let offset = offset
            .as_ref()
            .map(|offset| self.bind_expression(&offset.value));
        (limit, offset)
    }

    pub fn bind_order_by(&self, order_by_list: &Vec<OrderByExpr>) -> Vec<BoundOrderBy> {
        order_by_list
            .iter()
            .map(|expr| BoundOrderBy {
                expression: self.bind_expression(&expr.expr),
                desc: expr.asc.map_or(false, |asc| !asc),
            })
            .collect::<Vec<BoundOrderBy>>()
    }
}
