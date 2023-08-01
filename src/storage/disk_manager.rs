use std::io::{Read, Seek, Write};
use std::mem;
use std::sync::{Arc, Mutex};

use crate::common::config::TINYSQL_PAGE_SIZE;

use super::page::PageId;

#[derive(Debug, Clone)]
pub struct DiskManager {
    inner: Arc<Mutex<Inner>>
}

#[derive(Debug)]
struct Inner {
    db_path: String,
    db_file: std::fs::File,
    next_page_id: PageId,
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
            inner: Arc::new(Mutex::new(Inner {
                db_path,
                db_file,
                next_page_id,
            })),
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&self, page_id: PageId) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        let mut guard = self.inner.lock().unwrap();

        guard.db_file
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

        Self::_writer_page(&mut guard, page_id, data)
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> PageId {
        let mut guard = self.inner.lock().unwrap();

        let next_id = guard.next_page_id + 1;
        let page_id = mem::replace(&mut guard.next_page_id, next_id);

        Self::_writer_page(&mut guard, page_id, &[0; TINYSQL_PAGE_SIZE]);

        page_id
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        // TODO 利用pageId或者释放的空间
        self.write_page(page_id, &[0; TINYSQL_PAGE_SIZE]);
    }

    fn _writer_page(inner: &mut Inner, page_id: PageId, data: &[u8]) {
        inner.db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        inner.db_file.write_all(data).unwrap();
    }
}