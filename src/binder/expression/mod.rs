use self::column_ref::BoundColumnRef;

pub mod column_ref;

#[derive(Debug)]
pub enum BoundExpression {
    Invalid,
    ColumnRef(BoundColumnRef),
}
