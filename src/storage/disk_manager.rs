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
            next_page_id: AtomicU32::new(next_page_id),
            inner: Mutex::new(Inner { db_file }),
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&self, page_id: PageId) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut guard = self.inner.lock().unwrap();
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        guard
            .db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        guard.db_file.read_exact(&mut buf).unwrap();
        buf
    }

    // 将数据写入磁盘指定页
    pub fn write_page(&self, page_id: PageId, data: &[u8]) {
        let mut guard = self.inner.lock().unwrap();
        Self::_write_page(&mut guard, page_id, data);
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> PageId {
        let mut guard = self.inner.lock().unwrap();
        let page_id = self.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self::_write_page(&mut guard, page_id, &[0; TINYSQL_PAGE_SIZE]);
        page_id
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        // TODO 利用pageId或者释放的空间
        let mut guard = self.inner.lock().unwrap();
        Self::_write_page(&mut guard, page_id, &[0; TINYSQL_PAGE_SIZE]);
    }

    fn _write_page(guard: &mut MutexGuard<Inner>, page_id: PageId, data: &[u8]) {
        guard
            .db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        guard.db_file.write_all(data).unwrap();
    }
}

unsafe impl Send for DiskManager {}
unsafe impl Sync for DiskManager {}
