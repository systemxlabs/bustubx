use crate::{BustubxError, BustubxResult};
use std::u16;

pub trait Serializable: Sized {
    fn serialize(&self) -> BustubxResult<Vec<u8>>;

    fn deserialize(bytes: &[u8]) -> BustubxResult<(Self, &[u8])>;
}

impl Serializable for u16 {
    fn serialize(&self) -> BustubxResult<Vec<u8>> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> BustubxResult<(Self, &[u8])> {
        if bytes.len() < 2 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                2
            )));
        }
        let data = [bytes[0], bytes[1]];
        Ok((u16::from_be_bytes(data), &bytes[2..]))
    }
}

impl Serializable for u32 {
    fn serialize(&self) -> BustubxResult<Vec<u8>> {
        Ok(self.to_ne_bytes().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> BustubxResult<(Self, &[u8])> {
        if bytes.len() < 4 {
            return Err(BustubxError::Storage(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((u32::from_ne_bytes(data), &bytes[4..]))
    }
}
