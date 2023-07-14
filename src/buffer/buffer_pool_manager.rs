use std::collections::{HashMap, VecDeque};

use crate::storage::{disk_manager::DiskManager, page::{Page, PageId}};

use super::replacer::LRUKReplacer;

// TODO 暂时定义在这里
pub type FrameId = u32;

pub struct BufferPoolManager {
    pool: HashMap<PageId, Page>,
    // LRU-K置换算法
    replacer: LRUKReplacer,
    disk_manager: DiskManager,
    // 缓冲池中的页号与frame号的映射
    page_table: HashMap<PageId, FrameId>,
    // 缓冲池中空闲的frame
    free_list: VecDeque<FrameId>,
    // 缓冲池中的页数
    num_pages: usize,
    // 下一次分配的页号
    next_page_id: PageId,
}
impl BufferPoolManager {
    pub fn new(num_pages: usize, disk_manager: DiskManager) -> Self {
        let mut free_list = VecDeque::with_capacity(num_pages);
        for i in 0..num_pages {
            free_list.push_back(i as FrameId);
        }
        Self {
            pool: HashMap::new(),
            replacer: LRUKReplacer::new(num_pages, 2),
            disk_manager,
            page_table: HashMap::new(),
            free_list,
            num_pages,
            next_page_id: 0,
        }
    }
}