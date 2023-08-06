use crate::{catalog::column::DataType, dbtype::value::Value};

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
    pub fn to_value(&self, data_type: DataType) -> Value {
        match self {
            Constant::Number(n) => match data_type {
                DataType::TinyInt => Value::TinyInt(n.parse::<i8>().unwrap()),
                DataType::SmallInt => Value::SmallInt(n.parse::<i16>().unwrap()),
                DataType::Integer => Value::Integer(n.parse::<i32>().unwrap()),
                _ => unimplemented!(),
            },
            Constant::Boolean(b) => Value::Boolean(*b),
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
    pub fn evaluate(&self) -> Value {
        match &self.value {
            Constant::Number(n) => Value::Integer(n.parse::<i32>().unwrap()),
            Constant::Boolean(b) => Value::Boolean(*b),
            _ => unimplemented!(),
        }
    }
}
