use crate::common::{ScalarValue, TableReference};
use crate::expression::{AggregateFunction, BinaryExpr, ColumnExpr, Expr, Literal};
use crate::function::AggregateFunctionKind;
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
                Ok(Expr::Binary(BinaryExpr {
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
            sqlparser::ast::Expr::Function(function) => self.bind_function(function),
            _ => Err(BustubxError::NotSupport(format!(
                "sqlparser expr {} not supported",
                sql
            ))),
        }
    }

    pub fn bind_value(&self, value: &sqlparser::ast::Value) -> BustubxResult<Expr> {
        match value {
            sqlparser::ast::Value::Number(s, _) => {
                if let Ok(num) = s.parse::<i64>() {
                    return Ok(Expr::Literal(Literal { value: num.into() }));
                }
                if let Ok(num) = s.parse::<f64>() {
                    return Ok(Expr::Literal(Literal { value: num.into() }));
                }
                Err(BustubxError::Internal(
                    "Failed to parse sql number value".to_string(),
                ))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(Expr::Literal(Literal { value: (*b).into() })),
            sqlparser::ast::Value::Null => Ok(Expr::Literal(Literal {
                value: ScalarValue::Int8(None),
            })),
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(Literal {
                value: s.clone().into(),
            })),
            _ => Err(BustubxError::NotSupport(format!(
                "sqlparser value {} not supported",
                value
            ))),
        }
    }

    pub fn bind_function(&self, function: &sqlparser::ast::Function) -> BustubxResult<Expr> {
        let name = function.name.to_string();

        if let Some(func_kind) = AggregateFunctionKind::find(name.as_str()) {
            let args = function
                .args
                .iter()
                .map(|arg| self.bind_function_arg(arg))
                .collect::<BustubxResult<Vec<Expr>>>()?;
            return Ok(Expr::AggregateFunction(AggregateFunction {
                func_kind,
                args,
                distinct: function.distinct,
            }));
        }

        Err(BustubxError::Plan(format!(
            "The function {} is not supported",
            function
        )))
    }

    pub fn bind_function_arg(&self, arg: &sqlparser::ast::FunctionArg) -> BustubxResult<Expr> {
        match arg {
            sqlparser::ast::FunctionArg::Named {
                name: _,
                arg: sqlparser::ast::FunctionArgExpr::Expr(arg),
            } => self.bind_expr(arg),
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(arg)) => {
                self.bind_expr(arg)
            }
            _ => Err(BustubxError::Plan(format!(
                "The function arg {} is not supported",
                arg
            ))),
        }
    }
}
