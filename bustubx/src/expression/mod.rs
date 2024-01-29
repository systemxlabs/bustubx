use crate::catalog::DataType;
use crate::catalog::Schema;
use crate::common::ScalarValue;
use crate::error::BustubxResult;
use crate::storage::Tuple;

mod alias;
mod binary;
mod column;
mod literal;

pub use alias::Alias;
pub use binary::{BinaryExpr, BinaryOp};
pub use column::ColumnExpr;
pub use literal::Literal;

pub trait ExprTrait {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType>;

    /// Evaluate an expression against a Tuple
    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue>;
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Alias),
    /// A named reference to a qualified filed in a schema.
    Column(ColumnExpr),
    /// A constant value.
    Literal(Literal),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
}

impl ExprTrait for Expr {
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        match self {
            Expr::Alias(alias) => alias.data_type(input_schema),
            Expr::Column(column) => column.data_type(input_schema),
            Expr::Literal(literal) => literal.data_type(input_schema),
            Expr::BinaryExpr(binary) => binary.data_type(input_schema),
        }
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        match self {
            Expr::Alias(alias) => alias.evaluate(tuple),
            Expr::Column(column) => column.evaluate(tuple),
            Expr::Literal(literal) => literal.evaluate(tuple),
            Expr::BinaryExpr(binary) => binary.evaluate(tuple),
        }
    }
}
