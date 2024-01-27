use crate::catalog::data_type::DataType;
use crate::catalog::schema::Schema;
use crate::common::scalar::ScalarValue;
use crate::error::BustubxResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::Tuple;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub name: String,
}

impl ExprTrait for Alias {
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        self.expr.data_type(input_schema)
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        self.expr.evaluate(tuple)
    }
}
