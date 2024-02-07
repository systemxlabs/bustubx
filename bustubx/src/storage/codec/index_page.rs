use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::index_page::BPlusTreePageType;
use crate::{BustubxError, BustubxResult};

pub struct BPlusTreePageTypeCodec;

impl BPlusTreePageTypeCodec {
    pub fn encode(page_type: &BPlusTreePageType) -> Vec<u8> {
        match page_type {
            BPlusTreePageType::LeafPage => CommonCodec::encode_u32(1),
            BPlusTreePageType::InternalPage => CommonCodec::encode_u32(2),
        }
    }

    pub fn decode(bytes: &[u8]) -> BustubxResult<DecodedData<BPlusTreePageType>> {
        let (flag, offset) = CommonCodec::decode_u32(bytes)?;
        match flag {
            1 => Ok((BPlusTreePageType::LeafPage, offset)),
            2 => Ok((BPlusTreePageType::InternalPage, offset)),
            _ => Err(BustubxError::Storage(format!("Invalid page type {}", flag))),
        }
    }
}
