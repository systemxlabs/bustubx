use std::io::{Read, Seek, Write};

use crate::common::config::TINYSQL_PAGE_SIZE;

use super::page::PageId;

#[derive(Debug)]
pub struct DiskManager {
    pub db_path: String,
    pub db_file: std::fs::File,
    pub next_page_id: PageId,
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
            db_file,
            next_page_id,
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&mut self, page_id: PageId) -> [u8; TINYSQL_PAGE_SIZE] {
        let mut buf = [0; TINYSQL_PAGE_SIZE];
        self.db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        self.db_file.read_exact(&mut buf).unwrap();
        buf
    }

    // 将数据写入磁盘指定页
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) {
        self.db_file
            .seek(std::io::SeekFrom::Start(
                (page_id as usize * TINYSQL_PAGE_SIZE) as u64,
            ))
            .unwrap();
        self.db_file.write_all(data).unwrap();
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        self.write_page(page_id, &[0; TINYSQL_PAGE_SIZE]);
        page_id
    }

    pub fn deallocate_page(&mut self, page_id: PageId) {
        // TODO 利用pageId或者释放的空间
        self.write_page(page_id, &[0; TINYSQL_PAGE_SIZE]);
    }
}
