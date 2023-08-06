use crate::{catalog::schema::Schema, dbtype::value::Value, storage::tuple::Tuple};

use self::{
    alias::BoundAlias, binary_op::BoundBinaryOp, column_ref::BoundColumnRef,
    constant::BoundConstant,
};

pub mod alias;
pub mod binary_op;
pub mod column_ref;
pub mod constant;

#[derive(Debug, Clone)]
pub enum BoundExpression {
    Constant(BoundConstant),
    ColumnRef(BoundColumnRef),
    BinaryOp(BoundBinaryOp),
    Alias(BoundAlias),
}
impl BoundExpression {
    pub fn evaluate(&self, tuple: Option<&Tuple>, schema: Option<&Schema>) -> Value {
        match self {
            BoundExpression::Constant(c) => c.evaluate(),
            BoundExpression::ColumnRef(c) => c.evaluate(tuple, schema),
            BoundExpression::BinaryOp(b) => b.evaluate(tuple, schema),
            BoundExpression::Alias(a) => a.evaluate(tuple, schema),
            _ => unimplemented!(),
        }
    }
}
