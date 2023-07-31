use crate::{
    buffer::buffer_pool::BufferPoolManager,
    common::{config::INVALID_PAGE_ID, rid::Rid},
};

use super::{
    page::PageId,
    table_page::{self, TablePage},
    tuple::{Tuple, TupleMeta},
};

#[derive(Debug)]
pub struct TableHeap {
    pub buffer_pool_manager: BufferPoolManager,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
}
impl TableHeap {
    pub fn new(mut buffer_pool_manager: BufferPoolManager) -> Self {
        // new一个page并初始化
        let first_page = buffer_pool_manager
            .new_page()
            .expect("Can not new page for table heap");
        let first_page_id = first_page.page_id;
        let table_page = TablePage::new(INVALID_PAGE_ID);
        first_page.data = table_page.to_bytes();
        buffer_pool_manager.unpin_page(first_page_id, true);

        Self {
            buffer_pool_manager,
            first_page_id: first_page_id,
            last_page_id: first_page_id,
        }
    }

    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<Rid> {
        let mut last_page_id = self.last_page_id;
        let last_page = self
            .buffer_pool_manager
            .fetch_page(self.last_page_id)
            .expect("Can not fetch last page");
        let mut last_table_page = TablePage::from_bytes(&last_page.data);
        loop {
            if last_table_page.get_next_tuple_offset(meta, tuple).is_some() {
                break;
            }
            assert!(
                last_table_page.num_tuples > 0,
                "tuple is too large, cannot insert"
            );

            // 申请新的page
            let next_page = self
                .buffer_pool_manager
                .new_page()
                .expect("cannot allocate page");
            let next_page_id = next_page.page_id;
            let next_table_page = TablePage::new(INVALID_PAGE_ID);
            next_page.data = next_table_page.to_bytes();

            // 更新并释放上一个page
            last_table_page.next_page_id = next_page_id;
            self.buffer_pool_manager
                .write_page(last_page_id, last_table_page.to_bytes());
            self.buffer_pool_manager.unpin_page(last_page_id, true);

            // 更新last_page_id
            last_page_id = next_page_id;
            last_table_page = next_table_page;
            self.last_page_id = last_page_id;
        }
        let slot_id = last_table_page.insert_tuple(meta, tuple);
        self.buffer_pool_manager
            .write_page(last_page_id, last_table_page.to_bytes());
        self.buffer_pool_manager.unpin_page(last_page_id, true);
        slot_id.map(|slot_id| Rid::new(last_page_id, slot_id as u32))
    }

    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: Rid) {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let mut table_page = TablePage::from_bytes(&page.data);
        table_page.update_tuple_meta(meta, &rid);
        page.data = table_page.to_bytes();
        self.buffer_pool_manager.unpin_page(rid.page_id, true);
    }

    pub fn get_tuple(&mut self, rid: Rid) -> (TupleMeta, Tuple) {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let mut table_page = TablePage::from_bytes(&page.data);
        let result = table_page.get_tuple(&rid);
        self.buffer_pool_manager.unpin_page(rid.page_id, false);
        result
    }

    pub fn get_tuple_meta(&mut self, rid: Rid) -> TupleMeta {
        let page = self
            .buffer_pool_manager
            .fetch_page(rid.page_id)
            .expect("Can not fetch page");
        let mut table_page = TablePage::from_bytes(&page.data);
        let result = table_page.get_tuple_meta(&rid);
        self.buffer_pool_manager.unpin_page(rid.page_id, false);
        result
    }
}

mod tests {
    use std::fs::remove_file;

    use crate::{
        buffer::buffer_pool::BufferPoolManager,
        storage::{disk_manager, table_heap::TableHeap, tuple::Tuple},
    };

    #[test]
    pub fn test_table_heap_new() {
        let db_path = "./test_table_heap_new.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(10, disk_manager);
        let table_heap = TableHeap::new(buffer_pool_manager);
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);
        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 1);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_table_heap_insert_tuple() {
        let db_path = "./test_table_heap_insert_tuple.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(1000, disk_manager);
        let mut table_heap = TableHeap::new(buffer_pool_manager);
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };

        table_heap.insert_tuple(&meta, &Tuple::new(vec![1; 2000]));
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);
        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 1);

        table_heap.insert_tuple(&meta, &Tuple::new(vec![1; 2000]));
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 0);
        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 1);

        table_heap.insert_tuple(&meta, &Tuple::new(vec![1; 2000]));
        assert_eq!(table_heap.first_page_id, 0);
        assert_eq!(table_heap.last_page_id, 1);
        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 2);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_table_heap_update_tuple_meta() {
        let db_path = "./test_table_heap_update_tuple_meta.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(1000, disk_manager);
        let mut table_heap = TableHeap::new(buffer_pool_manager);
        let meta = super::TupleMeta {
            insert_txn_id: 0,
            delete_txn_id: 0,
            is_deleted: false,
        };

        let rid1 = table_heap
            .insert_tuple(&meta, &Tuple::new(vec![1; 2000]))
            .unwrap();
        let rid2 = table_heap
            .insert_tuple(&meta, &Tuple::new(vec![2; 2000]))
            .unwrap();
        let rid3 = table_heap
            .insert_tuple(&meta, &Tuple::new(vec![3; 2000]))
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
        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 2);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_table_heap_get_tuple() {
        let db_path = "./test_table_heap_get_tuple.db";
        let _ = remove_file(db_path);

        let disk_manager = disk_manager::DiskManager::new(db_path.to_string());
        let buffer_pool_manager = BufferPoolManager::new(1000, disk_manager);
        let mut table_heap = TableHeap::new(buffer_pool_manager);

        let meta1 = super::TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 1,
            is_deleted: false,
        };
        let rid1 = table_heap
            .insert_tuple(&meta1, &Tuple::new(vec![1; 2000]))
            .unwrap();
        let meta2 = super::TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let rid2 = table_heap
            .insert_tuple(&meta2, &Tuple::new(vec![2; 2000]))
            .unwrap();
        let meta3 = super::TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 3,
            is_deleted: false,
        };
        let rid3 = table_heap
            .insert_tuple(&meta3, &Tuple::new(vec![3; 2000]))
            .unwrap();

        let (meta, tuple) = table_heap.get_tuple(rid1);
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1; 2000]);

        let (meta, tuple) = table_heap.get_tuple(rid2);
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2; 2000]);

        let (meta, tuple) = table_heap.get_tuple(rid3);
        assert_eq!(meta, meta3);
        assert_eq!(tuple.data, vec![3; 2000]);

        assert_eq!(table_heap.buffer_pool_manager.replacer.size(), 2);

        let _ = remove_file(db_path);
    }
}
