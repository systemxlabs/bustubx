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
    Invalid,
    Constant(BoundConstant),
    ColumnRef(BoundColumnRef),
    BinaryOp(BoundBinaryOp),
    Alias(BoundAlias),
}
