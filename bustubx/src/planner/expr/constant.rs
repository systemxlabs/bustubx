use crate::catalog::DataType;
use crate::common::ScalarValue;

#[derive(Debug, Clone)]
pub enum Constant {
    Number(String),
    Null,
    Boolean(bool),
}
impl Constant {
    pub fn from_sqlparser_value(value: &sqlparser::ast::Value) -> Self {
        match value {
            sqlparser::ast::Value::Number(n, ..) => Constant::Number(n.to_string()),
            sqlparser::ast::Value::Boolean(b) => Constant::Boolean(*b),
            sqlparser::ast::Value::Null => Constant::Null,
            _ => unimplemented!(),
        }
    }
    pub fn to_value(&self, data_type: DataType) -> ScalarValue {
        match self {
            Constant::Number(n) => match data_type {
                DataType::Int8 => ScalarValue::Int8(Some(n.parse::<i8>().unwrap())),
                DataType::Int16 => ScalarValue::Int16(Some(n.parse::<i16>().unwrap())),
                DataType::Int32 => ScalarValue::Int32(Some(n.parse::<i32>().unwrap())),
                DataType::Int64 => ScalarValue::Int64(Some(n.parse::<i64>().unwrap())),
                _ => unimplemented!(),
            },
            Constant::Boolean(b) => ScalarValue::Boolean(Some(*b)),
            _ => unimplemented!(),
        }
    }
}

/// A bound constant, e.g., `1`.
#[derive(Debug, Clone)]
pub struct BoundConstant {
    pub value: Constant,
}
impl BoundConstant {
    pub fn evaluate(&self) -> ScalarValue {
        match &self.value {
            Constant::Number(n) => ScalarValue::Int32(Some(n.parse::<i32>().unwrap())),
            Constant::Boolean(b) => ScalarValue::Boolean(Some(*b)),
            _ => unimplemented!(),
        }
    }
}
