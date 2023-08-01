use std::{
    cell::UnsafeCell,
    io::{Read, Seek, Write},
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU32, Arc, Mutex, RwLock},
};

use crate::common::config::TINYSQL_PAGE_SIZE;

use super::page::PageId;

#[derive(Debug)]
pub struct DiskManager {
    pub db_path: String,
    pub db_file: UnsafeCell<std::fs::File>,
    pub next_page_id: AtomicU32,
    db_io_latch: Mutex<()>,
}
impl DiskManager {
    pub fn new(db_path: String) -> Self {
        let db_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .unwrap();
        let next_page_id = db_file
            .metadata()
            .unwrap()
            .len()
            .div_euclid(TINYSQL_PAGE_SIZE as u64) as PageId;
        println!("Initialized disk_manager next_page_id: {}", next_page_id);
        Self {
            db_path,
            db_file: UnsafeCell::new(db_file),
            next_page_id: AtomicU32::new(next_page_id),
            db_io_latch: Mutex::new(()),
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&self, page_id: PageId) -> [u8; TINYSQL_PAGE_SIZE] {
        let lock = self.db_io_latch.lock().unwrap();
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        unsafe {
            (*self.db_file.get())
                .seek(std::io::SeekFrom::Start(
                    (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
                ))
                .unwrap();
            (*self.db_file.get()).read_exact(&mut buf).unwrap();
        }
        drop(lock);
        buf
    }

    // 将数据写入磁盘指定页
    pub fn write_page(&self, page_id: PageId, data: &[u8]) {
        let lock = self.db_io_latch.lock().unwrap();
        unsafe {
            (*self.db_file.get())
                .seek(std::io::SeekFrom::Start(
                    (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
                ))
                .unwrap();
            (*self.db_file.get()).write_all(data).unwrap();
        }
        drop(lock);
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> PageId {
        let lock = self.db_io_latch.lock().unwrap();
        let page_id = self.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        unsafe {
            (*self.db_file.get())
                .seek(std::io::SeekFrom::Start(
                    (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
                ))
                .unwrap();
            (*self.db_file.get())
                .write_all(&[0; TINYSQL_PAGE_SIZE])
                .unwrap();
        }
        drop(lock);
        page_id
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        // TODO 利用pageId或者释放的空间
        let lock = self.db_io_latch.lock().unwrap();
        unsafe {
            (*self.db_file.get())
                .seek(std::io::SeekFrom::Start(
                    (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
                ))
                .unwrap();
            (*self.db_file.get())
                .write_all(&[0; TINYSQL_PAGE_SIZE])
                .unwrap();
        }
        drop(lock);
    }
}

unsafe impl Send for DiskManager {}
unsafe impl Sync for DiskManager {}
