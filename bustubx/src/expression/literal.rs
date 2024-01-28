use crate::catalog::data_type::DataType;
use crate::catalog::schema::Schema;
use crate::common::scalar::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::ExprTrait;
use crate::storage::tuple::Tuple;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    value: ScalarValue,
}

impl ExprTrait for Literal {
    fn data_type(&self, _input_schema: &Schema) -> BustubxResult<DataType> {
        Ok(self.value.data_type())
    }

    fn evaluate(&self, _tuple: &Tuple) -> BustubxResult<ScalarValue> {
        Ok(self.value.clone())
    }
}