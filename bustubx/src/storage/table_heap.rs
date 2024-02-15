use crate::buffer::{PageId, BUSTUBX_PAGE_SIZE, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::storage::codec::TablePageCodec;
use crate::{buffer::BufferPoolManager, common::rid::Rid, BustubxResult};

use super::{
    table_page::TablePage,
    tuple::{Tuple, TupleMeta},
};

#[derive(Debug)]
pub struct TableHeap {
    pub schema: SchemaRef,
    pub buffer_pool_manager: BufferPoolManager,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
}

impl TableHeap {
    pub fn try_new(
        schema: SchemaRef,
        mut buffer_pool_manager: BufferPoolManager,
    ) -> BustubxResult<Self> {
        // new a page and initialize
        let first_page = buffer_pool_manager.new_page()?;
        let first_page_id = first_page.read().unwrap().page_id;
        let table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        let mut data = [0u8; BUSTUBX_PAGE_SIZE];
        data.copy_from_slice(&TablePageCodec::encode(&table_page));
        first_page.write().unwrap().data = data;
        buffer_pool_manager.unpin_page(first_page_id, true)?;

        Ok(Self {
            schema,
            buffer_pool_manager,
            first_page_id,
            last_page_id: first_page_id,
        })
    }

    /// Inserts a tuple into the table.
    ///
    /// This function inserts the given tuple into the table. If the last page in the table
    /// has enough space for the tuple, it is inserted there. Otherwise, a new page is allocated
    /// and the tuple is inserted there.
    ///
    /// Parameters:
    /// - `meta`: The metadata associated with the tuple.
    /// - `tuple`: The tuple to be inserted.
    ///
    /// Returns:
    /// An `Option` containing the `Rid` of the inserted tuple if successful, otherwise `None`.
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<Rid> {
        let mut last_page_id = self.last_page_id;
        let last_page = self
            .buffer_pool_manager
            .fetch_page(self.last_page_id)
            .expect("Can not fetch last page");

        // Loop until a suitable page is found for inserting the tuple
        let (mut last_table_page, _) =
            TablePageCodec::decode(&last_page.read().unwrap().data, self.schema.clone()).unwrap();
        loop {
            if last_table_page.get_next_tuple_offset(meta, tuple).is_some() {
                break;
            }

            // if there's no tuple in the page, and we can't insert the tuple,
            // then this tuple is too large.
            assert!(
                last_table_page.header.num_tuples > 0,
                "tuple is too large, cannot insert"
            );

            // Allocate a new page if no more table pages are available.
            let next_page = self
                .buffer_pool_manager
                .new_page()
                .expect("cannot allocate page");
            let next_page_id = next_page.read().unwrap().page_id;
            let next_table_page = TablePage::new(self.schema.clone(), INVALID_PAGE_ID);
            let mut data = [0u8; BUSTUBX_PAGE_SIZE];
            data.copy_from_slice(&TablePageCodec::encode(&next_table_page));
            next_page.write().unwrap().data = data;

            // Update and release the previous page
            last_table_page.header.next_page_id = next_page_id;

            let mut data = [0u8; BUSTUBX_PAGE_SIZE];
            data.copy_from_slice(&TablePageCodec::encode(&last_table_page));

            self.buffer_pool_manager.write_page(last_page_id, data);
            self.buffer_pool_manager
                .unpin_page(last_page_id, true)
                .unwrap();

            // Update last_page_id.
            last_page_id = next_page_id;
            last_table_page = next_table_page;
            self.last_page_id = last_page_id;
        }

        // Insert the tuple into the chosen page
        let slot_id = last_table_page.insert_tuple(meta, tuple);

        let mut data = [0u8; BUSTUBX_PAGE_SIZE];
        data.copy_from_slice(&TablePageCodec::encode(&last_table_page));

        self.buffer_pool_manager.write_page(last_page_id, data);
        self.buffer_pool_manager
            .unpin_page(last_page_id, true)
            .unwrap();

        // Map the slot_id to a Rid and return
        slot_id.map(|slot_id| Rid::new(last_page_id, slot_id as u32))
    }

    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: Rid) {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let (mut table_page, _) =
            TablePageCodec::decode(&page.read().unwrap().data, self.schema.clone()).unwrap();
        table_page.update_tuple_meta(meta, &rid);

        let mut data = [0u8; BUSTUBX_PAGE_SIZE];
        data.copy_from_slice(&TablePageCodec::encode(&table_page));

        page.write().unwrap().data = data;
        self.buffer_pool_manager
            .unpin_page(rid.page_id, true)
            .unwrap();
    }

    pub fn get_tuple(&mut self, rid: Rid) -> (TupleMeta, Tuple) {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let (mut table_page, _) =
            TablePageCodec::decode(&page.read().unwrap().data, self.schema.clone()).unwrap();
        let result = table_page.get_tuple(&rid);
        self.buffer_pool_manager
            .unpin_page(rid.page_id, false)
            .unwrap();
        result
    }

    pub fn get_tuple_meta(&mut self, rid: Rid) -> TupleMeta {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let (mut table_page, _) =
            TablePageCodec::decode(&page.read().unwrap().data, self.schema.clone()).unwrap();
        let result = table_page.get_tuple_meta(&rid);
        self.buffer_pool_manager
            .unpin_page(rid.page_id, false)
            .unwrap();
        result
    }

    pub fn get_first_rid(&mut self) -> Option<Rid> {
        let page = self
            .buffer_pool_manager
            .fetch_page(self.first_page_id)
            .expect("Can not fetch page");
        let (table_page, _) =
            TablePageCodec::decode(&page.read().unwrap().data, self.schema.clone()).unwrap();
        self.buffer_pool_manager
            .unpin_page(self.first_page_id, false)
            .unwrap();
        if table_page.header.num_tuples == 0 {
            // TODO 忽略删除的tuple
            return None;
        } else {
            return Some(Rid::new(self.first_page_id, 0));
        }
    }

    pub fn get_next_rid(&mut self, rid: Rid) -> Option<Rid> {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let (table_page, _) =
            TablePageCodec::decode(&page.read().unwrap().data, self.schema.clone()).unwrap();
        self.buffer_pool_manager
            .unpin_page(rid.page_id, false)
            .unwrap();
        let next_rid = table_page.get_next_rid(&rid);
        if next_rid.is_some() {
            return next_rid;
        }

        if table_page.header.next_page_id == INVALID_PAGE_ID {
            return None;
        }
        let next_page = self
            .buffer_pool_manager
            .fetch_page(table_page.header.next_page_id)
            .expect("Can not fetch page");
        let (next_table_page, _) =
            TablePageCodec::decode(&next_page.read().unwrap().data, self.schema.clone()).unwrap();
        self.buffer_pool_manager
            .unpin_page(table_page.header.next_page_id, false)
            .unwrap();
        if next_table_page.header.num_tuples == 0 {
            // TODO 忽略删除的tuple
            return None;
        } else {
            return Some(Rid::new(table_page.header.next_page_id, 0));
        }
    }

    pub fn iter(&mut self, start_at: Option<Rid>, stop_at: Option<Rid>) -> TableIterator {
        TableIterator {
            rid: start_at.or(self.get_first_rid()),
            stop_at,
        }
    }
}

#[derive(derive_new::new, Debug)]
pub struct TableIterator {
    pub rid: Option<Rid>,
    pub stop_at: Option<Rid>,
}

impl TableIterator {
    pub fn next(&mut self, table_heap: &mut TableHeap) -> Option<(TupleMeta, Tuple)> {
        if self.rid.is_none() {
            return None;
        }
        let rid = self.rid.unwrap();
        if self.stop_at.is_some() && rid == self.stop_at.unwrap() {
            return None;
        }
        let result = table_heap.get_tuple(rid);
        self.rid = table_heap.get_next_rid(rid);
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::catalog::{Column, DataType, Schema};
    use crate::{
        buffer::BufferPoolManager,
        storage::{table_heap::TableHeap, DiskManager, Tuple},
    };

    #[test]
    pub fn test_table_heap_new() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(&temp_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(10, Arc::new(disk_manager));
        let table_heap =
            TableHeap::try_new(Arc::new(Schema::empty()), buffer_pool_manager).unwrap();
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);
    }

    #[test]
    pub fn test_table_heap_insert_tuple() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(&temp_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut table_heap = TableHeap::try_new(schema.clone(), buffer_pool_manager).unwrap();
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };

        table_heap.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
        );
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);

        table_heap.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
        );
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);

        table_heap.insert_tuple(
            &meta,
            &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
        );
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);
    }

    #[test]
    pub fn test_table_heap_update_tuple_meta() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(&temp_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut table_heap = TableHeap::try_new(schema.clone(), buffer_pool_manager).unwrap();
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };

        let rid1 = table_heap
            .insert_tuple(
                &meta,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let rid2 = table_heap
            .insert_tuple(
                &meta,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let rid3 = table_heap
            .insert_tuple(
                &meta,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut meta = table_heap.get_tuple_meta(rid2);
        meta.insert_txn_id = 1;
        meta.delete_txn_id = 2;
        meta.is_deleted = true;
        table_heap.update_tuple_meta(&meta, rid2);

        let meta = table_heap.get_tuple_meta(rid2);
        assert_eq!(meta.insert_txn_id, 1);
        assert_eq!(meta.delete_txn_id, 2);
        assert_eq!(meta.is_deleted, true);
    }

    #[test]
    pub fn test_table_heap_get_tuple() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(&temp_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut table_heap = TableHeap::try_new(schema.clone(), buffer_pool_manager).unwrap();

        let meta1 = super::TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 1,
            is_deleted: false,
        };
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 3,
            is_deleted: false,
        };
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let (meta, tuple) = table_heap.get_tuple(rid1);
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (meta, tuple) = table_heap.get_tuple(rid2);
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (meta, tuple) = table_heap.get_tuple(rid3);
        assert_eq!(meta, meta3);
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);
    }

    #[test]
    pub fn test_table_heap_iterator() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));

        let disk_manager = DiskManager::try_new(&temp_path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(1000, Arc::new(disk_manager));
        let mut table_heap = TableHeap::try_new(schema.clone(), buffer_pool_manager).unwrap();

        let meta1 = super::TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 1,
            is_deleted: false,
        };
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 3,
            is_deleted: false,
        };
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut iterator = table_heap.iter(None, None);

        let (meta, tuple) = iterator.next(&mut table_heap).unwrap();
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (meta, tuple) = iterator.next(&mut table_heap).unwrap();
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (meta, tuple) = iterator.next(&mut table_heap).unwrap();
        assert_eq!(meta, meta3);
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);

        assert!(iterator.next(&mut table_heap).is_none());
    }
}
