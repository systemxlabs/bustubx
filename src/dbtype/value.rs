use std::fmt::Formatter;

use crate::catalog::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    // NULL is less than any non-NULL values
    // Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
}
impl Value {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(Self::boolean_from_bytes(bytes)),
            DataType::Int8 => Self::TinyInt(i8::from_be_bytes([bytes[0]])),
            DataType::Int16 => Self::SmallInt(i16::from_be_bytes([bytes[0], bytes[1]])),
            DataType::Int32 => {
                Self::Integer(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }
            DataType::Int64 => Self::BigInt(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])),
            _ => panic!("Not implemented"),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Boolean(v) => Self::boolean_to_bytes(*v),
            Self::TinyInt(v) => v.to_be_bytes().to_vec(),
            Self::SmallInt(v) => v.to_be_bytes().to_vec(),
            Self::Integer(v) => v.to_be_bytes().to_vec(),
            Self::BigInt(v) => v.to_be_bytes().to_vec(),
        }
    }

    pub fn from_sqlparser_value(value: &sqlparser::ast::Value, data_type: DataType) -> Self {
        match value {
            sqlparser::ast::Value::Number(v, _) => match data_type {
                DataType::Int8 => Self::TinyInt(v.parse::<i8>().unwrap()),
                DataType::Int16 => Self::SmallInt(v.parse::<i16>().unwrap()),
                DataType::Int32 => Self::Integer(v.parse::<i32>().unwrap()),
                DataType::Int64 => Self::BigInt(v.parse::<i64>().unwrap()),
                _ => panic!("Not implemented"),
            },
            // sqlparser::ast::Value::SingleQuotedString(_) => {}
            sqlparser::ast::Value::Boolean(b) => Value::Boolean(*b),
            _ => unreachable!(),
        }
    }

    // TODO compare value with different data type
    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match self {
            Self::Boolean(v1) => match other {
                Self::Boolean(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::TinyInt(v1) => match other {
                Self::TinyInt(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::SmallInt(v1) => match other {
                Self::SmallInt(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::Integer(v1) => match other {
                Self::Integer(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::BigInt(v1) => match other {
                Self::BigInt(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
        }
    }

    pub fn boolean_from_bytes(bytes: &[u8]) -> bool {
        bytes[0] != 0
    }
    pub fn boolean_to_bytes(value: bool) -> Vec<u8> {
        if value {
            vec![1]
        } else {
            vec![0]
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Value::Boolean(e) => write!(f, "{}", e)?,
            Value::TinyInt(e) => write!(f, "{}", e)?,
            Value::SmallInt(e) => write!(f, "{}", e)?,
            Value::Integer(e) => write!(f, "{}", e)?,
            Value::BigInt(e) => write!(f, "{}", e)?,
        };
        Ok(())
    }
}
