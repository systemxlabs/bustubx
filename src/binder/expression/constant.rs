use crate::{
    catalog::column::DataType,
    dbtype::{
        boolean::Boolean, integer::Integer, smallint::SmallInt, tinyint::TinyInt, value::Value,
    },
};

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
                DataType::TinyInt => Value::TinyInt(TinyInt::new(n.parse::<i8>().unwrap())),
                DataType::SmallInt => Value::SmallInt(SmallInt::new(n.parse::<i16>().unwrap())),
                DataType::Integer => Value::Integer(Integer::new(n.parse::<i32>().unwrap())),
                _ => unimplemented!(),
            },
            Constant::Boolean(b) => Value::Boolean(Boolean::new(*b)),
            _ => unimplemented!(),
        }
    }
}

/// A bound constant, e.g., `1`.
#[derive(Debug, Clone)]
pub struct BoundConstant {
    pub value: Constant,
}
