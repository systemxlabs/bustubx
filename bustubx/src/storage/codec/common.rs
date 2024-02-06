use crate::storage::codec::DecodedData;
use crate::{BustubxError, BustubxResult};

pub struct CommonCodec;

impl CommonCodec {
    pub fn encode_u8(data: u8) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u8(bytes: &[u8]) -> BustubxResult<DecodedData<u8>> {
        if bytes.len() < 1 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                1
            )));
        }
        Ok((u8::from_be_bytes([bytes[0]]), 1))
    }

    pub fn encode_u16(data: u16) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u16(bytes: &[u8]) -> BustubxResult<DecodedData<u16>> {
        if bytes.len() < 2 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                2
            )));
        }
        let data = [bytes[0], bytes[1]];
        Ok((u16::from_be_bytes(data), 2))
    }

    pub fn encode_u32(data: u32) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u32(bytes: &[u8]) -> BustubxResult<DecodedData<u32>> {
        if bytes.len() < 4 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((u32::from_be_bytes(data), 4))
    }

    pub fn encode_u64(data: u64) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u64(bytes: &[u8]) -> BustubxResult<DecodedData<u64>> {
        if bytes.len() < 8 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                8
            )));
        }
        let data = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok((u64::from_be_bytes(data), 8))
    }

    pub fn encode_i8(data: i8) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i8(bytes: &[u8]) -> BustubxResult<DecodedData<i8>> {
        if bytes.len() < 1 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                1
            )));
        }
        Ok((i8::from_be_bytes([bytes[0]]), 1))
    }

    pub fn encode_i16(data: i16) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i16(bytes: &[u8]) -> BustubxResult<DecodedData<i16>> {
        if bytes.len() < 2 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                2
            )));
        }
        let data = [bytes[0], bytes[1]];
        Ok((i16::from_be_bytes(data), 2))
    }

    pub fn encode_i32(data: i32) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i32(bytes: &[u8]) -> BustubxResult<DecodedData<i32>> {
        if bytes.len() < 4 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((i32::from_be_bytes(data), 4))
    }

    pub fn encode_i64(data: i64) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i64(bytes: &[u8]) -> BustubxResult<DecodedData<i64>> {
        if bytes.len() < 8 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                8
            )));
        }
        let data = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok((i64::from_be_bytes(data), 8))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::codec::CommonCodec;

    #[test]
    fn common_codec() {
        assert_eq!(
            5u8,
            CommonCodec::decode_u8(&CommonCodec::encode_u8(5u8))
                .unwrap()
                .0
        );
        assert_eq!(
            5u16,
            CommonCodec::decode_u16(&CommonCodec::encode_u16(5u16))
                .unwrap()
                .0
        );
        assert_eq!(
            5u32,
            CommonCodec::decode_u32(&CommonCodec::encode_u32(5u32))
                .unwrap()
                .0
        );
        assert_eq!(
            5u64,
            CommonCodec::decode_u64(&CommonCodec::encode_u64(5u64))
                .unwrap()
                .0
        );

        assert_eq!(
            5i8,
            CommonCodec::decode_i8(&CommonCodec::encode_i8(5i8))
                .unwrap()
                .0
        );
        assert_eq!(
            5i16,
            CommonCodec::decode_i16(&CommonCodec::encode_i16(5i16))
                .unwrap()
                .0
        );
        assert_eq!(
            5i32,
            CommonCodec::decode_i32(&CommonCodec::encode_i32(5i32))
                .unwrap()
                .0
        );
        assert_eq!(
            5i64,
            CommonCodec::decode_i64(&CommonCodec::encode_i64(5i64))
                .unwrap()
                .0
        );
    }
}
