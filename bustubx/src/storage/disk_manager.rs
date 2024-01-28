use std::fs::File;
use std::path::Path;
use std::{
    io::{Read, Seek, Write},
    sync::{atomic::AtomicU32, Mutex, MutexGuard},
};

use crate::common::config::BUSTUBX_PAGE_SIZE;
use crate::error::BustubxResult;

use super::page::PageId;

static EMPTY_PAGE: [u8; BUSTUBX_PAGE_SIZE] = [0; BUSTUBX_PAGE_SIZE];

#[derive(Debug)]
pub struct DiskManager {
    pub next_page_id: AtomicU32,
    db_file: Mutex<File>,
}

impl DiskManager {
    pub fn try_new(db_path: impl AsRef<Path>) -> BustubxResult<Self> {
        // Create a file handle db_file using the OpenOptions struct from the Rust standard library.
        // By chaining calls, we set the read, write and create modes of the file.
        let db_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)?;

        // Gets the metadata of the database file, including the size of the file.
        // Then it divides the file size by the page size.
        // Convert the result to a page number PageId.
        // Note: This is the next available page number.
        let next_page_id = db_file
            .metadata()?
            .len()
            .div_euclid(BUSTUBX_PAGE_SIZE as u64) as PageId;
        println!("Initialized disk_manager next_page_id: {}", next_page_id);

        Ok(Self {
            next_page_id: AtomicU32::new(next_page_id),
            // Use a mutex to wrap the file handle to ensure that only one thread
            // can access the file at the same time among multiple threads.
            db_file: Mutex::new(db_file),
        })
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&self, page_id: PageId) -> BustubxResult<[u8; BUSTUBX_PAGE_SIZE]> {
        let mut guard = self.db_file.lock().unwrap();
        let mut buf = [0; BUSTUBX_PAGE_SIZE];

        // guard.db_file is a file object, set the file pointer to
        // the specified position through the .seek(...) method.
        // Specifically, locate the file pointer to the starting
        // position of the corresponding page.
        // Here ... should be a suitable offset.
        guard.seek(std::io::SeekFrom::Start(
            (page_id as usize * BUSTUBX_PAGE_SIZE) as u64,
        ))?;
        // Read buf.len() bytes of data from the file, and store the data in the buf array.
        guard.read_exact(&mut buf)?;

        Ok(buf)
    }

    // 将数据写入磁盘指定页
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> BustubxResult<()> {
        assert_eq!(data.len(), BUSTUBX_PAGE_SIZE);
        let mut guard = self.db_file.lock().unwrap();
        Self::write_page_internal(&mut guard, page_id, data)
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> BustubxResult<PageId> {
        let mut guard = self.db_file.lock().unwrap();

        // Load the current value of next_page_id using atomic load operation.
        // Increment the next_page_id by 1 using atomic fetch_add operation.
        let page_id = self.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Write an empty page (all zeros) to the allocated page.
        Self::write_page_internal(&mut guard, page_id, &EMPTY_PAGE)?;

        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> BustubxResult<()> {
        // TODO 利用pageId或者释放的空间
        // TODO 添加单测
        let mut guard = self.db_file.lock().unwrap();

        // Write an empty page (all zeros) to the deallocated page.
        // But this page is not deallocated, only data will be written with null or zeros.
        Self::write_page_internal(&mut guard, page_id, &EMPTY_PAGE)
    }

    fn write_page_internal(
        guard: &mut MutexGuard<File>,
        page_id: PageId,
        data: &[u8],
    ) -> BustubxResult<()> {
        // Seek to the start of the page in the database file and write the data.
        guard
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * BUSTUBX_PAGE_SIZE) as u64,
            ))
            .unwrap();
        guard.write_all(data)?;
        Ok(())
    }

    pub fn db_file_len(&self) -> BustubxResult<u64> {
        let guard = self.db_file.lock().unwrap();
        let meta = guard.metadata()?;
        Ok(meta.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::config::BUSTUBX_PAGE_SIZE;

    #[test]
    pub fn test_disk_manager_allocate_page() {
        let db_path = "test_disk_manager_allocate_page.db";
        let _ = std::fs::remove_file(db_path);

        let disk_manager = super::DiskManager::try_new(&db_path).unwrap();

        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 0);
        let page = disk_manager.read_page(page_id).unwrap();
        assert_eq!(page, [0; 4096]);

        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 1);
        let page = disk_manager.read_page(page_id).unwrap();
        assert_eq!(page, [0; 4096]);

        let db_file_len = disk_manager.db_file_len().unwrap();
        assert_eq!(db_file_len, 8192);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_disk_manager_write_read_page() {
        let db_path = "test_disk_manager_write_page.db";
        let _ = std::fs::remove_file(db_path);

        let disk_manager = super::DiskManager::try_new(&db_path).unwrap();

        let page_id1 = disk_manager.allocate_page().unwrap();
        let page_id2 = disk_manager.allocate_page().unwrap();

        let mut page1 = vec![1, 2, 3];
        page1.extend(vec![0; BUSTUBX_PAGE_SIZE - 3]);
        disk_manager.write_page(page_id1, &page1).unwrap();
        let page = disk_manager.read_page(page_id1).unwrap();
        assert_eq!(page, page1.as_slice());

        let mut page2 = vec![0; BUSTUBX_PAGE_SIZE - 3];
        page2.extend(vec![1, 2, 3]);
        disk_manager.write_page(page_id2, &page2).unwrap();
        let page = disk_manager.read_page(page_id2).unwrap();
        assert_eq!(page, page2.as_slice());

        let _ = std::fs::remove_file(db_path);
    }
}
