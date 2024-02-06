use crate::catalog::{Column, DataType, Schema};
use crate::common::ScalarValue;
use crate::expression::{Expr, ExprTrait};
use crate::{BustubxError, BustubxResult, Tuple};

/// Cast expression
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl ExprTrait for Cast {
    fn data_type(&self, _input_schema: &Schema) -> BustubxResult<DataType> {
        Ok(self.data_type)
    }

    fn nullable(&self, input_schema: &Schema) -> BustubxResult<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        let value = self.expr.evaluate(tuple)?;
        value.cast_to(&self.data_type)
    }

    fn to_column(&self, input_schema: &Schema) -> BustubxResult<Column> {
        Err(BustubxError::Plan(format!(
            "expr {:?} as column not supported",
            self
        )))
    }
}

impl std::fmt::Display for Cast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST {} AS {}", self.expr, self.data_type)
    }
}
