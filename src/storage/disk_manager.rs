use std::{
    io::{Read, Seek, Write},
    sync::{atomic::AtomicU32, Mutex, MutexGuard},
};

use crate::common::config::TINYSQL_PAGE_SIZE;

use super::page::PageId;

#[derive(Debug)]
pub struct DiskManager {
    pub db_path: String,
    pub next_page_id: AtomicU32,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    db_file: std::fs::File,
}

impl DiskManager {
    pub fn new(db_path: String) -> Self {
        // Create a file handle db_file using the OpenOptions struct from the Rust standard library.
        // By chaining calls, we set the read, write and create modes of the file.
        let db_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .unwrap();


        // Gets the metadata of the database file, including the size of the file.
        // Then it divides the file size by the page size.
        // Convert the result to a page number PageId.
        // Note: This is the next available page number.
        let next_page_id = db_file
            .metadata()
            .unwrap()
            .len()
            .div_euclid(TINYSQL_PAGE_SIZE as u64) as PageId;
        println!("Initialized disk_manager next_page_id: {}", next_page_id);

        Self {
            db_path,
            next_page_id: AtomicU32::new(next_page_id),
            // Use a mutex to wrap the file handle to ensure that only one thread
            // can access the file at the same time among multiple threads.
            inner: Mutex::new(Inner { db_file }),
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&self, page_id: PageId) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut guard = self.inner.lock().unwrap();
        let mut buf = [0; TINYSQL_PAGE_SIZE];

        // guard.db_file is a file object, set the file pointer to
        // the specified position through the .seek(...) method.
        // Specifically, locate the file pointer to the starting
        // position of the corresponding page.
        // Here ... should be a suitable offset.
        guard.db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        // Read buf.len() bytes of data from the file, and store the data in the buf array.
        guard.db_file.read_exact(&mut buf).unwrap();

        buf
    }

    // 将数据写入磁盘指定页
    pub fn write_page(&self, page_id: PageId, data: &[u8]) {
        assert_eq!(data.len(), TINYSQL_PAGE_SIZE);
        let mut guard = self.inner.lock().unwrap();
        Self::_write_page(&mut guard, page_id, data);
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> PageId {
        let mut guard = self.inner.lock().unwrap();

        // Load the current value of next_page_id using atomic load operation.
        // Increment the next_page_id by 1 using atomic fetch_add operation.
        let page_id = self.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Write an empty page (all zeros) to the allocated page.
        Self::_write_page(&mut guard, page_id, &[0; TINYSQL_PAGE_SIZE]);

        page_id
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        // TODO 利用pageId或者释放的空间
        // TODO 添加单测
        let mut guard = self.inner.lock().unwrap();

        // Write an empty page (all zeros) to the deallocated page.
        // But this page is not deallocated, only data will be written with null or zeros.
        Self::_write_page(&mut guard, page_id, &[0; TINYSQL_PAGE_SIZE]);
    }

    fn _write_page(guard: &mut MutexGuard<Inner>, page_id: PageId, data: &[u8]) {
        // Seek to the start of the page in the database file and write the data.
        guard
            .db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        guard.db_file.write_all(data).unwrap();
    }

    pub fn db_file_len(&self) -> u64 {
        let guard = self.inner.lock().unwrap();
        guard.db_file.metadata().unwrap().len()
    }
}

mod tests {
    use std::io::{Read, Seek, Write};

    use crate::common::config::TINYSQL_PAGE_SIZE;

    #[test]
    pub fn test_disk_manager_allocate_page() {
        let db_path = "test_disk_manager_allocate_page.db";
        let _ = std::fs::remove_file(db_path);

        let disk_manager = super::DiskManager::new(db_path.to_string());

        let page_id = disk_manager.allocate_page();
        assert_eq!(page_id, 0);
        let page = disk_manager.read_page(page_id);
        assert_eq!(page, [0; 4096]);

        let page_id = disk_manager.allocate_page();
        assert_eq!(page_id, 1);
        let page = disk_manager.read_page(page_id);
        assert_eq!(page, [0; 4096]);

        let db_file_len = disk_manager.db_file_len();
        assert_eq!(db_file_len, 8192);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_disk_manager_write_read_page() {
        let db_path = "test_disk_manager_write_page.db";
        let _ = std::fs::remove_file(db_path);

        let disk_manager = super::DiskManager::new(db_path.to_string());

        let page_id1 = disk_manager.allocate_page();
        let page_id2 = disk_manager.allocate_page();

        let mut page1 = vec![1, 2, 3];
        page1.extend(vec![0; TINYSQL_PAGE_SIZE - 3]);
        disk_manager.write_page(page_id1, &page1);
        let page = disk_manager.read_page(page_id1);
        assert_eq!(page, page1.as_slice());

        let mut page2 = vec![0; TINYSQL_PAGE_SIZE - 3];
        page2.extend(vec![1, 2, 3]);
        disk_manager.write_page(page_id2, &page2);
        let page = disk_manager.read_page(page_id2);
        assert_eq!(page, page2.as_slice());

        let _ = std::fs::remove_file(db_path);
    }
}
