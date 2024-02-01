use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::common::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::ExprTrait;
use crate::storage::Tuple;
use crate::BustubxError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub value: ScalarValue,
}

impl ExprTrait for Literal {
    fn data_type(&self, _input_schema: &Schema) -> BustubxResult<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &Schema) -> BustubxResult<bool> {
        Ok(self.value.is_null())
    }

    fn evaluate(&self, _tuple: &Tuple) -> BustubxResult<ScalarValue> {
        Ok(self.value.clone())
    }

    fn to_column(&self, _input_schema: &Schema) -> BustubxResult<Column> {
        Err(BustubxError::Plan(format!(
            "expr {:?} as column not supported",
            self
        )))
    }
}
