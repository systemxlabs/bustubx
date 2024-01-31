use crate::catalog::{DataType, Schema};
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
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        Ok(self.data_type)
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        let value = self.expr.evaluate(tuple)?;
        match value {
            ScalarValue::Boolean(v) => match self.data_type {
                DataType::Boolean => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
            ScalarValue::Int8(v) => match self.data_type {
                DataType::Int8 => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
            ScalarValue::Int16(v) => match self.data_type {
                DataType::Int16 => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
            ScalarValue::Int32(v) => match self.data_type {
                DataType::Int32 => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
            ScalarValue::Int64(v) => match self.data_type {
                DataType::Int64 => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
            ScalarValue::UInt64(v) => match self.data_type {
                DataType::Int32 => Ok(v.map(|v| v as i32).into()),
                DataType::UInt64 => Ok(value),
                _ => Err(BustubxError::Internal(format!(
                    "Failed to cast {} as {}",
                    value, self.data_type
                ))),
            },
        }
    }
}
