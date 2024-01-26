use std::fmt::Formatter;

use crate::catalog::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    // NULL is less than any non-NULL values
    // Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
}
impl ScalarValue {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(Self::boolean_from_bytes(bytes)),
            DataType::Int8 => Self::Int8(i8::from_be_bytes([bytes[0]])),
            DataType::Int16 => Self::Int16(i16::from_be_bytes([bytes[0], bytes[1]])),
            DataType::Int32 => {
                Self::Int32(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }
            DataType::Int64 => Self::Int64(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])),
            _ => panic!("Not implemented"),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Boolean(v) => Self::boolean_to_bytes(*v),
            Self::Int8(v) => v.to_be_bytes().to_vec(),
            Self::Int16(v) => v.to_be_bytes().to_vec(),
            Self::Int32(v) => v.to_be_bytes().to_vec(),
            Self::Int64(v) => v.to_be_bytes().to_vec(),
        }
    }

    pub fn from_sqlparser_value(value: &sqlparser::ast::Value, data_type: DataType) -> Self {
        match value {
            sqlparser::ast::Value::Number(v, _) => match data_type {
                DataType::Int8 => Self::Int8(v.parse::<i8>().unwrap()),
                DataType::Int16 => Self::Int16(v.parse::<i16>().unwrap()),
                DataType::Int32 => Self::Int32(v.parse::<i32>().unwrap()),
                DataType::Int64 => Self::Int64(v.parse::<i64>().unwrap()),
                _ => panic!("Not implemented"),
            },
            // sqlparser::ast::Value::SingleQuotedString(_) => {}
            sqlparser::ast::Value::Boolean(b) => ScalarValue::Boolean(*b),
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
            Self::Int8(v1) => match other {
                Self::Int8(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::Int16(v1) => match other {
                Self::Int16(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::Int32(v1) => match other {
                Self::Int32(v2) => v1.cmp(v2),
                _ => panic!("Not implemented"),
            },
            Self::Int64(v1) => match other {
                Self::Int64(v2) => v1.cmp(v2),
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

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScalarValue::Boolean(e) => write!(f, "{}", e)?,
            ScalarValue::Int8(e) => write!(f, "{}", e)?,
            ScalarValue::Int16(e) => write!(f, "{}", e)?,
            ScalarValue::Int32(e) => write!(f, "{}", e)?,
            ScalarValue::Int64(e) => write!(f, "{}", e)?,
        };
        Ok(())
    }
}
