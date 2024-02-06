use crate::catalog::DataType;
use crate::{BustubxError, BustubxResult};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt64(Option<u64>),
}

impl ScalarValue {
    pub fn new_empty(data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(None),
            DataType::Int8 => Self::Int8(None),
            DataType::Int16 => Self::Int16(None),
            DataType::Int32 => Self::Int32(None),
            DataType::Int64 => Self::Int64(None),
            DataType::UInt64 => Self::UInt64(None),
        }
    }
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(Some(Self::boolean_from_bytes(bytes))),
            DataType::Int8 => Self::Int8(Some(i8::from_be_bytes([bytes[0]]))),
            DataType::Int16 => Self::Int16(Some(i16::from_be_bytes([bytes[0], bytes[1]]))),
            DataType::Int32 => Self::Int32(Some(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))),
            DataType::Int64 => Self::Int64(Some(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))),
            DataType::UInt64 => Self::UInt64(Some(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Boolean(Some(v)) => Self::boolean_to_bytes(*v),
            Self::Int8(Some(v)) => v.to_be_bytes().to_vec(),
            Self::Int16(Some(v)) => v.to_be_bytes().to_vec(),
            Self::Int32(Some(v)) => v.to_be_bytes().to_vec(),
            Self::Int64(Some(v)) => v.to_be_bytes().to_vec(),
            Self::UInt64(Some(v)) => v.to_be_bytes().to_vec(),

            // TODO fixme
            Self::Boolean(None) => vec![0u8; 1],
            Self::Int8(None) => vec![0u8; 1],
            Self::Int16(None) => vec![0u8; 2],
            Self::Int32(None) => vec![0u8; 4],
            Self::Int64(None) => vec![0u8; 8],
            Self::UInt64(None) => vec![0u8; 8],
            _ => unimplemented!(),
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

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt64(_) => DataType::UInt64,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
        }
    }

    /// Try to cast this value to a ScalarValue of type `data_type`
    pub fn cast_to(&self, data_type: &DataType) -> BustubxResult<Self> {
        match data_type {
            DataType::Boolean => match self {
                ScalarValue::Boolean(v) => Ok(ScalarValue::Boolean(v.clone())),
                _ => Err(BustubxError::NotSupport(format!(
                    "Failed to cast {} to {} type",
                    self, data_type
                ))),
            },
            DataType::Int32 => match self {
                ScalarValue::Int64(v) => Ok(ScalarValue::Int32(v.map(|v| v as i32))),
                _ => Err(BustubxError::NotSupport(format!(
                    "Failed to cast {} to {} type",
                    self, data_type
                ))),
            },
            _ => Err(BustubxError::NotSupport(format!(
                "Not support cast to {} type",
                data_type
            ))),
        }
    }

    pub fn wrapping_add(&self, other: Self) -> BustubxResult<Self> {
        todo!()
    }

    pub fn wrapping_sub(&self, other: Self) -> BustubxResult<Self> {
        todo!()
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
        }
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ScalarValue::Boolean(None) => write!(f, "NULL"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int8(None) => write!(f, "NULL"),
            ScalarValue::Int8(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int16(None) => write!(f, "NULL"),
            ScalarValue::Int16(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int32(None) => write!(f, "NULL"),
            ScalarValue::Int32(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int64(None) => write!(f, "NULL"),
            ScalarValue::Int64(Some(v)) => write!(f, "{v}"),
            ScalarValue::UInt64(None) => write!(f, "NULL"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{v}"),
        }
    }
}

macro_rules! impl_from_for_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_from_for_scalar!(i8, Int8);
impl_from_for_scalar!(i16, Int16);
impl_from_for_scalar!(i32, Int32);
impl_from_for_scalar!(i64, Int64);
impl_from_for_scalar!(u64, UInt64);
impl_from_for_scalar!(bool, Boolean);
