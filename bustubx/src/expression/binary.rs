use crate::catalog::DataType;
use crate::catalog::Schema;
use crate::common::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::Tuple;

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
        todo!()
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
