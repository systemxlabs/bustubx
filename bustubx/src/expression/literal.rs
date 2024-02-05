use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::common::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::ExprTrait;
use crate::storage::Tuple;

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

    fn to_column(&self, input_schema: &Schema) -> BustubxResult<Column> {
        Ok(Column::new(
            format!("{}", self.value),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
