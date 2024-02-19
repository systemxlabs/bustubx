use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::meta_page::MetaPage;
use crate::BustubxResult;

pub struct MetaPageCodec;

impl MetaPageCodec {
    pub fn encode(page: &MetaPage) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(CommonCodec::encode_u32(page.major_version));
        bytes.extend(CommonCodec::encode_u32(page.minor_version));
        bytes.extend(CommonCodec::encode_u32(page.freelist_page_id));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> BustubxResult<DecodedData<MetaPage>> {
        let mut left_bytes = bytes;

        let (major_version, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (minor_version, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (freelist_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            MetaPage {
                major_version,
                minor_version,
                freelist_page_id,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::codec::MetaPageCodec;
    use crate::storage::MetaPage;

    #[test]
    fn meta_page_codec() {
        let page = MetaPage::try_new().unwrap();
        let (new_page, _) = MetaPageCodec::decode(&MetaPageCodec::encode(&page)).unwrap();
        assert_eq!(page, new_page);
    }
}
