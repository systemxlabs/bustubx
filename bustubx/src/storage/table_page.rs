use crate::buffer::{PageId, BUSTUBX_PAGE_SIZE};
use crate::catalog::SchemaRef;
use crate::common::rid::Rid;
use crate::storage::codec::{TablePageHeaderCodec, TablePageHeaderTupleInfoCodec, TupleCodec};

use super::tuple::{Tuple, TupleMeta};

lazy_static::lazy_static! {
    pub static ref EMPTY_TUPLE_INFO: TupleInfo = TupleInfo {
        offset: 0,
        size: 0,
        meta: TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        }
    };
}

/**
 * Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | NextPageId (4)| NumTuples(2) | NumDeletedTuples(2) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size + TupleMeta | Tuple_2 offset+size + TupleMeta | ... |
 *  ----------------------------------------------------------------
 *
 */
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TablePage {
    pub schema: SchemaRef,
    pub header: TablePageHeader,
    // 整个页原始数据
    // TODO 可以通过memmove、memcpy优化，参考bustub
    pub data: [u8; BUSTUBX_PAGE_SIZE],
}

impl TablePage {
    pub fn new(schema: SchemaRef, next_page_id: PageId) -> Self {
        Self {
            schema,
            header: TablePageHeader {
                next_page_id,
                num_tuples: 0,
                num_deleted_tuples: 0,
                tuple_infos: Vec::new(),
            },
            data: [0; BUSTUBX_PAGE_SIZE],
        }
    }

    // Get the offset for the next tuple insertion.
    pub fn get_next_tuple_offset(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        // Get the ending offset of the current slot. If there are inserted tuples,
        // get the offset of the previous inserted tuple; otherwise, set it to the size of the page.
        let slot_end_offset = if self.header.num_tuples > 0 {
            self.header.tuple_infos[self.header.num_tuples as usize - 1].offset
        } else {
            BUSTUBX_PAGE_SIZE as u16
        };

        // Check if the current slot has enough space for the new tuple. Return None if not.
        if slot_end_offset < TupleCodec::encode(tuple).len() as u16 {
            return None;
        }

        // Calculate the insertion offset for the new tuple by subtracting its data length
        // from the ending offset of the current slot.
        let tuple_offset = slot_end_offset - TupleCodec::encode(tuple).len() as u16;

        // Calculate the minimum valid tuple insertion offset, including the table page header size,
        // the total size of each tuple info (existing tuple infos and newly added tuple info).
        let min_tuple_offset = TablePageHeaderCodec::encode(&self.header).len()
            + TablePageHeaderTupleInfoCodec::encode(&EMPTY_TUPLE_INFO).len();
        if (tuple_offset as usize) < min_tuple_offset {
            return None;
        }

        // Return the calculated insertion offset for the new tuple.
        return Some(tuple_offset);
    }

    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        // Get the offset for the next tuple insertion.
        let tuple_offset = self.get_next_tuple_offset(meta, tuple)?;
        let tuple_id = self.header.num_tuples;

        // Store tuple information including offset, length, and metadata.
        self.header.tuple_infos.push(TupleInfo {
            offset: tuple_offset,
            size: TupleCodec::encode(tuple).len() as u16,
            meta: meta.clone(),
        });

        // only check
        assert_eq!(tuple_id, self.header.tuple_infos.len() as u16 - 1);

        self.header.num_tuples += 1;
        if meta.is_deleted {
            self.header.num_deleted_tuples += 1;
        }

        // Copy the tuple's data into the appropriate position within the page's data buffer.
        self.data[tuple_offset as usize
            ..(tuple_offset + TupleCodec::encode(tuple).len() as u16) as usize]
            .copy_from_slice(&TupleCodec::encode(tuple));
        return Some(tuple_id);
    }

    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &Rid) {
        let tuple_id = rid.slot_num;
        if tuple_id >= self.header.num_tuples as u32 {
            panic!("tuple_id {} out of range", tuple_id);
        }
        if meta.is_deleted && !self.header.tuple_infos[tuple_id as usize].meta.is_deleted {
            self.header.num_deleted_tuples += 1;
        }

        self.header.tuple_infos[tuple_id as usize].meta = meta.clone();
    }

    pub fn get_tuple(&self, rid: &Rid) -> (TupleMeta, Tuple) {
        let tuple_id = rid.slot_num;
        if tuple_id >= self.header.num_tuples as u32 {
            panic!("tuple_id {} out of range", tuple_id);
        }

        let offset = self.header.tuple_infos[tuple_id as usize].offset;
        let size = self.header.tuple_infos[tuple_id as usize].size;
        let meta = self.header.tuple_infos[tuple_id as usize].meta;
        let (tuple, _) = TupleCodec::decode(
            &self.data[offset as usize..(offset + size) as usize],
            self.schema.clone(),
        )
        .unwrap();

        return (meta, tuple);
    }

    pub fn get_tuple_meta(&self, rid: &Rid) -> TupleMeta {
        let tuple_id = rid.slot_num;
        if tuple_id >= self.header.num_tuples as u32 {
            panic!("tuple_id {} out of range", tuple_id);
        }

        return self.header.tuple_infos[tuple_id as usize].meta.clone();
    }

    pub fn get_next_rid(&self, rid: &Rid) -> Option<Rid> {
        // TODO 忽略删除的tuple
        let tuple_id = rid.slot_num;
        if tuple_id + 1 >= self.header.num_tuples as u32 {
            return None;
        }

        return Some(Rid::new(rid.page_id, tuple_id + 1));
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TablePageHeader {
    pub next_page_id: PageId,
    pub num_tuples: u16,
    pub num_deleted_tuples: u16,
    pub tuple_infos: Vec<TupleInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TupleInfo {
    pub offset: u16,
    pub size: u16,
    pub meta: TupleMeta,
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::Tuple;
    use std::sync::Arc;

    #[test]
    pub fn test_table_page_get_tuple() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let mut table_page = super::TablePage::new(schema.clone(), 0);
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
        );
        assert_eq!(tuple_id, Some(0));
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
        );
        assert_eq!(tuple_id, Some(1));
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
        );
        assert_eq!(tuple_id, Some(2));

        let (tuple_meta, tuple) = table_page.get_tuple(&super::Rid::new(0, 0));
        assert_eq!(tuple_meta, meta);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);
        let (tuple_meta, tuple) = table_page.get_tuple(&super::Rid::new(0, 1));
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);
        let (tuple_meta, tuple) = table_page.get_tuple(&super::Rid::new(0, 2));
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);
    }

    #[test]
    pub fn test_table_page_update_tuple_meta() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let mut table_page = super::TablePage::new(schema.clone(), 0);
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
        );
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
        );
        let tuple_id = table_page.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
        );

        let mut tuple_meta = table_page.get_tuple_meta(&super::Rid::new(0, 0));
        tuple_meta.is_deleted = true;
        tuple_meta.delete_txn_id = 1;
        tuple_meta.insert_txn_id = 2;

        table_page.update_tuple_meta(&tuple_meta, &super::Rid::new(0, 0));
        let tuple_meta = table_page.get_tuple_meta(&super::Rid::new(0, 0));
        assert_eq!(tuple_meta.is_deleted, true);
        assert_eq!(tuple_meta.delete_txn_id, 1);
        assert_eq!(tuple_meta.insert_txn_id, 2);
    }
}
