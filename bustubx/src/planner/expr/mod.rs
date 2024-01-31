use crate::{catalog::Schema, common::ScalarValue, storage::Tuple};

use self::{alias::Alias, binary_op::BinaryOp, column_ref::ColumnRef, constant::BoundConstant};

pub mod alias;
pub mod binary_op;
pub mod column_ref;
pub mod constant;

#[derive(Debug, Clone)]
pub enum Expr {
    Alias(Alias),
    Constant(BoundConstant),
    ColumnRef(ColumnRef),
    BinaryOp(BinaryOp),
}
impl Expr {
    pub fn evaluate(&self, tuple: Option<&Tuple>) -> ScalarValue {
        match self {
            Expr::Constant(c) => c.evaluate(),
            Expr::ColumnRef(c) => c.evaluate(tuple),
            Expr::BinaryOp(b) => b.evaluate(tuple),
            Expr::Alias(a) => a.evaluate(tuple),
            _ => unimplemented!(),
        }
    }
}
