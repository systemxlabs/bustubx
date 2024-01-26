use crate::{catalog::schema::Schema, common::scalar::ScalarValue, storage::tuple::Tuple};

use super::Expr;

#[derive(Debug, Clone, Copy)]
pub enum BinaryOperator {
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
impl BinaryOperator {
    pub fn from_sqlparser_operator(op: &sqlparser::ast::BinaryOperator) -> Self {
        match op {
            sqlparser::ast::BinaryOperator::Plus => BinaryOperator::Plus,
            sqlparser::ast::BinaryOperator::Minus => BinaryOperator::Minus,
            sqlparser::ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
            sqlparser::ast::BinaryOperator::Divide => BinaryOperator::Divide,
            sqlparser::ast::BinaryOperator::Gt => BinaryOperator::Gt,
            sqlparser::ast::BinaryOperator::Lt => BinaryOperator::Lt,
            sqlparser::ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
            sqlparser::ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
            sqlparser::ast::BinaryOperator::Eq => BinaryOperator::Eq,
            sqlparser::ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
            sqlparser::ast::BinaryOperator::And => BinaryOperator::And,
            sqlparser::ast::BinaryOperator::Or => BinaryOperator::Or,
            _ => unimplemented!(),
        }
    }
}

/// A bound binary operator, e.g., `a+b`.
#[derive(Debug, Clone)]
pub struct BinaryOp {
    pub larg: Box<Expr>,
    pub op: BinaryOperator,
    pub rarg: Box<Expr>,
}
impl BinaryOp {
    pub fn evaluate(&self, tuple: Option<&Tuple>, schema: Option<&Schema>) -> ScalarValue {
        let l = self.larg.evaluate(tuple, schema);
        let r = self.rarg.evaluate(tuple, schema);
        match self.op {
            // BinaryOperator::Plus => l + r,
            // BinaryOperator::Minus => l - r,
            // BinaryOperator::Multiply => l * r,
            // BinaryOperator::Divide => l / r,
            BinaryOperator::Gt => {
                let order = l.compare(&r);
                ScalarValue::Boolean(order == std::cmp::Ordering::Greater)
            }
            BinaryOperator::Lt => {
                let order = l.compare(&r);
                ScalarValue::Boolean(order == std::cmp::Ordering::Less)
            }
            BinaryOperator::GtEq => {
                let order = l.compare(&r);
                ScalarValue::Boolean(
                    order == std::cmp::Ordering::Greater || order == std::cmp::Ordering::Equal,
                )
            }
            BinaryOperator::LtEq => {
                let order = l.compare(&r);
                ScalarValue::Boolean(
                    order == std::cmp::Ordering::Less || order == std::cmp::Ordering::Equal,
                )
            }
            BinaryOperator::Eq => {
                let order = l.compare(&r);
                ScalarValue::Boolean(order == std::cmp::Ordering::Equal)
            }
            BinaryOperator::NotEq => {
                let order = l.compare(&r);
                ScalarValue::Boolean(order != std::cmp::Ordering::Equal)
            }
            // BinaryOperator::And => l && r,
            // BinaryOperator::Or => l || r,
            _ => unimplemented!(),
        }
    }
}
