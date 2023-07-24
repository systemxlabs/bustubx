use crate::catalog::column::DataType;

use super::{smallint::SmallInt, tinyint::TinyInt};

pub enum Value {
    TinyInt(TinyInt),
    SmallInt(SmallInt),
}
impl Value {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match bytes.len() {
            1 => Self::TinyInt(TinyInt::from_bytes(bytes)),
            2 => Self::SmallInt(SmallInt::from_bytes(bytes)),
            _ => panic!("Not implemented"),
        }
    }

    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match self {
            Self::TinyInt(v1) => match other {
                Self::TinyInt(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
            Self::SmallInt(v1) => match other {
                Self::SmallInt(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
        }
    }
}
