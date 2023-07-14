use std::io::{Read, Seek, Write};

use crate::common::config::TINYSQL_PAGE_SIZE;

use super::page::PageId;

#[derive(Debug)]
pub struct DiskManager {
    pub db_path: String,
    pub db_file: std::fs::File,
}
impl DiskManager {
    pub fn new(db_path: String) -> Self {
        let db_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .unwrap();
        Self {
            db_path,
            db_file,
        }
    }

    // 读取磁盘指定页的数据
    pub fn read_page(&mut self, page_id: PageId) -> Vec<u8> {
        let mut buf = vec![0; TINYSQL_PAGE_SIZE];
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
}
