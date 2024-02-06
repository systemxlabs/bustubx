use crate::buffer::BUSTUBX_PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::table_page::{TABLE_PAGE_HEADER_SIZE, TABLE_PAGE_TUPLE_INFO_SIZE};
use crate::storage::{TablePage, TupleMeta};
use crate::BustubxResult;

pub struct TablePageCodec;

impl TablePageCodec {
    pub fn encode(page: &TablePage) -> Vec<u8> {
        let mut header_bytes = Vec::new();
        header_bytes.extend(CommonCodec::encode_u32(page.next_page_id));
        header_bytes.extend(CommonCodec::encode_u16(page.num_tuples));
        header_bytes.extend(CommonCodec::encode_u16(page.num_deleted_tuples));

        todo!()
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> BustubxResult<DecodedData<TablePage>> {
        todo!()
    }
}

pub struct TupleMetaCodec;

impl TupleMetaCodec {
    pub fn encode(meta: &TupleMeta) -> Vec<u8> {
        todo!()
    }
}
