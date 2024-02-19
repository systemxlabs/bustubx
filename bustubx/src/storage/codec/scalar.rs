use crate::catalog::DataType;
use crate::common::ScalarValue;
use crate::storage::codec::{CommonCodec, DecodedData};
use crate::BustubxResult;

pub struct ScalarValueCodec;

impl ScalarValueCodec {
    pub fn encode(value: &ScalarValue) -> Vec<u8> {
        match value {
            ScalarValue::Boolean(Some(v)) => CommonCodec::encode_bool(*v),
            ScalarValue::Int8(Some(v)) => CommonCodec::encode_i8(*v),
            ScalarValue::Int16(Some(v)) => CommonCodec::encode_i16(*v),
            ScalarValue::Int32(Some(v)) => CommonCodec::encode_i32(*v),
            ScalarValue::Int64(Some(v)) => CommonCodec::encode_i64(*v),
            ScalarValue::UInt64(Some(v)) => CommonCodec::encode_u64(*v),
            ScalarValue::Float32(Some(v)) => CommonCodec::encode_f32(*v),
            ScalarValue::Float64(Some(v)) => CommonCodec::encode_f64(*v),
            // null
            ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None) => vec![],
        }
    }

    pub fn decode(bytes: &[u8], data_type: DataType) -> BustubxResult<DecodedData<ScalarValue>> {
        match data_type {
            DataType::Boolean => {
                let (value, offset) = CommonCodec::decode_bool(bytes)?;
                Ok((ScalarValue::Boolean(Some(value)), offset))
            }
            DataType::Int8 => {
                let (value, offset) = CommonCodec::decode_i8(bytes)?;
                Ok((ScalarValue::Int8(Some(value)), offset))
            }
            DataType::Int16 => {
                let (value, offset) = CommonCodec::decode_i16(bytes)?;
                Ok((ScalarValue::Int16(Some(value)), offset))
            }
            DataType::Int32 => {
                let (value, offset) = CommonCodec::decode_i32(bytes)?;
                Ok((ScalarValue::Int32(Some(value)), offset))
            }
            DataType::Int64 => {
                let (value, offset) = CommonCodec::decode_i64(bytes)?;
                Ok((ScalarValue::Int64(Some(value)), offset))
            }
            DataType::UInt64 => {
                let (value, offset) = CommonCodec::decode_u64(bytes)?;
                Ok((ScalarValue::UInt64(Some(value)), offset))
            }
            DataType::Float32 => {
                let (value, offset) = CommonCodec::decode_f32(bytes)?;
                Ok((ScalarValue::Float32(Some(value)), offset))
            }
            DataType::Float64 => {
                let (value, offset) = CommonCodec::decode_f64(bytes)?;
                Ok((ScalarValue::Float64(Some(value)), offset))
            }
        }
    }
}
