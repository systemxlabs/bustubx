use crate::catalog::DataType;
use crate::catalog::Schema;
use crate::common::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::Tuple;
use crate::BustubxError;

/// Binary expression
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: BinaryOp,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl ExprTrait for BinaryExpr {
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        let left_type = self.left.data_type(input_schema)?;
        let right_type = self.right.data_type(input_schema)?;
        match self.op {
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::GtEq
            | BinaryOp::LtEq
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::And
            | BinaryOp::Or => Ok(DataType::Boolean),
            BinaryOp::Plus => todo!(),
            BinaryOp::Minus => todo!(),
            BinaryOp::Multiply => todo!(),
            BinaryOp::Divide => todo!(),
        }
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        let l = self.left.evaluate(tuple)?;
        let r = self.right.evaluate(tuple)?;
        match self.op {
            BinaryOp::Gt => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order == std::cmp::Ordering::Greater,
                )))
            }
            BinaryOp::Lt => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order == std::cmp::Ordering::Less,
                )))
            }
            BinaryOp::GtEq => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order == std::cmp::Ordering::Greater || order == std::cmp::Ordering::Equal,
                )))
            }
            BinaryOp::LtEq => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order == std::cmp::Ordering::Less || order == std::cmp::Ordering::Equal,
                )))
            }
            BinaryOp::Eq => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order == std::cmp::Ordering::Equal,
                )))
            }
            BinaryOp::NotEq => {
                let order = l.compare(&r);
                Ok(ScalarValue::Boolean(Some(
                    order != std::cmp::Ordering::Equal,
                )))
            }
            _ => Err(BustubxError::NotSupport(format!(
                "binary operator {:?} not support evaluating yet",
                self.op
            ))),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}

impl TryFrom<&sqlparser::ast::BinaryOperator> for BinaryOp {
    type Error = BustubxError;

    fn try_from(value: &sqlparser::ast::BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::BinaryOperator::Plus => Ok(BinaryOp::Plus),
            sqlparser::ast::BinaryOperator::Minus => Ok(BinaryOp::Minus),
            sqlparser::ast::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            sqlparser::ast::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            sqlparser::ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            sqlparser::ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            sqlparser::ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            sqlparser::ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            sqlparser::ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            sqlparser::ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            sqlparser::ast::BinaryOperator::And => Ok(BinaryOp::And),
            sqlparser::ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            _ => Err(BustubxError::NotSupport(format!(
                "sqlparser binary operator {} not supported",
                value
            ))),
        }
    }
}
