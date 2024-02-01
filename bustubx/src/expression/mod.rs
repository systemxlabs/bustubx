mod alias;
mod binary;
mod cast;
mod column;
mod literal;

pub use alias::Alias;
pub use binary::{BinaryExpr, BinaryOp};
pub use cast::Cast;
pub use column::ColumnExpr;
pub use literal::Literal;

use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::common::ScalarValue;
use crate::storage::Tuple;
use crate::{BustubxError, BustubxResult};

pub trait ExprTrait {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType>;

    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> BustubxResult<bool>;

    /// Evaluate an expression against a Tuple
    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue>;

    /// convert to a column with respect to a schema
    fn to_column(&self, input_schema: &Schema) -> BustubxResult<Column>;
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
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast),
}

impl ExprTrait for Expr {
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        match self {
            Expr::Alias(alias) => alias.data_type(input_schema),
            Expr::Column(column) => column.data_type(input_schema),
            Expr::Literal(literal) => literal.data_type(input_schema),
            Expr::BinaryExpr(binary) => binary.data_type(input_schema),
            Expr::Cast(cast) => cast.data_type(input_schema),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> BustubxResult<bool> {
        match self {
            Expr::Alias(alias) => alias.nullable(input_schema),
            Expr::Column(column) => column.nullable(input_schema),
            Expr::Literal(literal) => literal.nullable(input_schema),
            Expr::BinaryExpr(binary) => binary.nullable(input_schema),
            Expr::Cast(cast) => cast.nullable(input_schema),
        }
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        match self {
            Expr::Alias(alias) => alias.evaluate(tuple),
            Expr::Column(column) => column.evaluate(tuple),
            Expr::Literal(literal) => literal.evaluate(tuple),
            Expr::BinaryExpr(binary) => binary.evaluate(tuple),
            Expr::Cast(cast) => cast.evaluate(tuple),
        }
    }

    fn to_column(&self, input_schema: &Schema) -> BustubxResult<Column> {
        match self {
            Expr::Alias(alias) => alias.to_column(input_schema),
            Expr::Column(column) => column.to_column(input_schema),
            Expr::Literal(literal) => literal.to_column(input_schema),
            Expr::BinaryExpr(binary) => binary.to_column(input_schema),
            Expr::Cast(cast) => cast.to_column(input_schema),
        }
    }
}
