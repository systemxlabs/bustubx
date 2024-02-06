use crate::buffer::BUSTUBX_PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::table_page::{TablePageHeader, TupleInfo};
use crate::storage::{TablePage, TupleMeta};
use crate::BustubxResult;

pub struct TablePageCodec;

impl TablePageCodec {
    pub fn encode(page: &TablePage) -> Vec<u8> {
        let header_bytes = TablePageHeaderCodec::encode(&page.header);
        let mut all_bytes = page.data.clone();
        all_bytes[0..header_bytes.len()].copy_from_slice(&header_bytes);
        all_bytes.to_vec()
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> BustubxResult<DecodedData<TablePage>> {
        let (header, offset) = TablePageHeaderCodec::decode(bytes)?;
        let mut data = [0u8; BUSTUBX_PAGE_SIZE];
        data.copy_from_slice(&bytes[0..BUSTUBX_PAGE_SIZE]);
        Ok((
            TablePage {
                schema,
                header,
                data,
            },
            BUSTUBX_PAGE_SIZE,
        ))
    }
}

pub struct TablePageHeaderCodec;

impl TablePageHeaderCodec {
    pub fn encode(header: &TablePageHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(CommonCodec::encode_u32(header.next_page_id));
        bytes.extend(CommonCodec::encode_u16(header.num_tuples));
        bytes.extend(CommonCodec::encode_u16(header.num_deleted_tuples));
        for tuple_info in header.tuple_infos.iter() {
            bytes.extend(CommonCodec::encode_u16(tuple_info.offset));
            bytes.extend(CommonCodec::encode_u16(tuple_info.size));
            bytes.extend(CommonCodec::encode_u32(tuple_info.meta.insert_txn_id));
            bytes.extend(CommonCodec::encode_u32(tuple_info.meta.delete_txn_id));
            bytes.extend(CommonCodec::encode_bool(tuple_info.meta.is_deleted));
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> BustubxResult<DecodedData<TablePageHeader>> {
        let mut left_bytes = bytes;

        let (next_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (num_tuples, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (num_deleted_tuples, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let mut tuple_infos = vec![];
        for _ in 0..num_tuples {
            let (tuple_offset, offset) = CommonCodec::decode_u16(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            let (size, offset) = CommonCodec::decode_u16(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            let (insert_txn_id, offset) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            let (delete_txn_id, offset) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            let (is_deleted, offset) = CommonCodec::decode_bool(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            tuple_infos.push(TupleInfo {
                offset: tuple_offset,
                size,
                meta: TupleMeta {
                    insert_txn_id,
                    delete_txn_id,
                    is_deleted,
                },
            });
        }
        Ok((
            TablePageHeader {
                next_page_id,
                num_tuples,
                num_deleted_tuples,
                tuple_infos,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::INVALID_PAGE_ID;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::table_page::TablePageHeaderCodec;
    use crate::storage::codec::TablePageCodec;
    use crate::storage::{TablePage, TupleMeta};
    use crate::Tuple;
    use std::sync::Arc;

    #[test]
    fn table_page_codec() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, true),
            Column::new("b".to_string(), DataType::Int32, true),
        ]));
        let tuple1 = Tuple::new(schema.clone(), vec![1i8.into(), 1i32.into()]);
        let tuple1_meta = TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let tuple2 = Tuple::new(schema.clone(), vec![2i8.into(), 2i32.into()]);
        let tuple2_meta = TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 4,
            is_deleted: true,
        };

        let mut table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        table_page.insert_tuple(&tuple1_meta, &tuple1);
        table_page.insert_tuple(&tuple2_meta, &tuple2);

        let (new_page, _) =
            TablePageCodec::decode(&TablePageCodec::encode(&table_page), schema.clone()).unwrap();
        assert_eq!(new_page.schema, table_page.schema);
        assert_eq!(new_page.header, table_page.header);
        let header_size = TablePageHeaderCodec::encode(&table_page.header).len();
        assert_eq!(new_page.data[header_size..], table_page.data[header_size..]);
    }
}
