use crate::common::table_ref::TableReference;
use crate::expression::{BinaryExpr, ColumnExpr, Expr, Literal};
use crate::planner::LogicalPlanner;
use crate::{BustubxError, BustubxResult};

impl LogicalPlanner<'_> {
    pub fn bind_expr(&self, sql: &sqlparser::ast::Expr) -> BustubxResult<Expr> {
        match sql {
            sqlparser::ast::Expr::Identifier(ident) => Ok(Expr::Column(ColumnExpr {
                relation: None,
                name: ident.value.clone(),
            })),
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let left = Box::new(self.bind_expr(left)?);
                let right = Box::new(self.bind_expr(right)?);
                Ok(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: op.try_into()?,
                    right,
                }))
            }
            sqlparser::ast::Expr::Value(value) => self.bind_value(value),
            sqlparser::ast::Expr::CompoundIdentifier(idents) => match idents.as_slice() {
                [col] => Ok(Expr::Column(ColumnExpr {
                    relation: None,
                    name: col.value.clone(),
                })),
                [table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::bare(table.value.clone())),
                    name: col.value.clone(),
                })),
                [schema, table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::partial(
                        schema.value.clone(),
                        table.value.clone(),
                    )),
                    name: col.value.clone(),
                })),
                [catalog, schema, table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::full(
                        catalog.value.clone(),
                        schema.value.clone(),
                        table.value.clone(),
                    )),
                    name: col.value.clone(),
                })),
                _ => Err(BustubxError::NotSupport(format!(
                    "sqlparser expr CompoundIdentifier has more than 4 identifiers: {:?}",
                    idents
                ))),
            },
            _ => Err(BustubxError::NotSupport(format!(
                "sqlparser expr {} not supported",
                sql
            ))),
        }
    }

    pub fn bind_value(&self, value: &sqlparser::ast::Value) -> BustubxResult<Expr> {
        match value {
            sqlparser::ast::Value::Number(s, _) => {
                let num: i64 = s.parse::<i64>().map_err(|e| {
                    BustubxError::Internal("Failed to parse literal as i64".to_string())
                })?;
                Ok(Expr::Literal(Literal { value: num.into() }))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(Expr::Literal(Literal { value: (*b).into() })),
            _ => Err(BustubxError::NotSupport(format!(
                "sqlparser value {} not supported",
                value
            ))),
        }
    }
}
