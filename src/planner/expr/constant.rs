use crate::catalog::data_type::DataType;
use crate::common::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub enum Constant {
    Number(String),
    Null,
    Boolean(bool),
    SingleQuotedString(String),
}
impl Constant {
    pub fn from_sqlparser_value(value: &sqlparser::ast::Value) -> Self {
        match value {
            sqlparser::ast::Value::Number(n, ..) => Constant::Number(n.to_string()),
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Constant::SingleQuotedString(s.to_string())
            }
            sqlparser::ast::Value::Boolean(b) => Constant::Boolean(*b),
            sqlparser::ast::Value::Null => Constant::Null,
            _ => unimplemented!(),
        }
    }
    pub fn to_value(&self, data_type: DataType) -> ScalarValue {
        match self {
            Constant::Number(n) => match data_type {
                DataType::Int8 => ScalarValue::Int8(n.parse::<i8>().unwrap()),
                DataType::Int16 => ScalarValue::Int16(n.parse::<i16>().unwrap()),
                DataType::Int32 => ScalarValue::Int32(n.parse::<i32>().unwrap()),
                DataType::Int64 => ScalarValue::Int64(n.parse::<i64>().unwrap()),
                _ => unimplemented!(),
            },
            Constant::Boolean(b) => ScalarValue::Boolean(*b),
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
            Constant::Number(n) => ScalarValue::Int32(n.parse::<i32>().unwrap()),
            Constant::Boolean(b) => ScalarValue::Boolean(*b),
            _ => unimplemented!(),
        }
    }
}
