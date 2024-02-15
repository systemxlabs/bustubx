use std::sync::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::buffer::page::{Page, PageId};
use crate::buffer::BUSTUBX_PAGE_SIZE;
use crate::storage::DiskManager;
use crate::{BustubxError, BustubxResult};

use super::replacer::LRUKReplacer;

pub type FrameId = u32;

pub const TABLE_HEAP_BUFFER_POOL_SIZE: usize = 100;

#[derive(Debug)]
pub struct BufferPoolManager {
    pool: Vec<Arc<RwLock<Page>>>,
    // LRU-K置换算法
    pub replacer: LRUKReplacer,
    pub disk_manager: Arc<DiskManager>,
    // 缓冲池中的页号与frame号的映射
    page_table: HashMap<PageId, FrameId>,
    // 缓冲池中空闲的frame
    free_list: VecDeque<FrameId>,
    // 缓冲池中的页数
    num_pages: usize,
}
impl BufferPoolManager {
    pub fn new(num_pages: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut free_list = VecDeque::with_capacity(num_pages);
        for i in 0..num_pages {
            free_list.push_back(i as FrameId);
        }
        Self {
            pool: vec![Arc::new(RwLock::new(Page::new(0))); num_pages],
            replacer: LRUKReplacer::new(num_pages, 2),
            disk_manager,
            page_table: HashMap::new(),
            free_list,
            num_pages,
        }
    }

    // 从缓冲池创建一个新页
    pub fn new_page(&mut self) -> BustubxResult<Arc<RwLock<Page>>> {
        // 缓冲池已满且无可替换的页
        if self.free_list.is_empty() && self.replacer.size() == 0 {
            return Err(BustubxError::Storage("Failed to new page".to_string()));
        }

        // 分配一个frame
        let frame_id = if !self.free_list.is_empty() {
            // 有空闲frame则直接分配
            self.free_list.pop_front().unwrap()
        } else {
            // 无空闲frame，从缓冲池中替换一个页
            if let Some(frame_id) = self.replacer.evict() {
                let evicted_page = self.pool[frame_id as usize].clone();
                let evicted_page_id = evicted_page.read().unwrap().page_id;
                // 如果页被修改过，则将其写回磁盘
                let is_dirty = evicted_page.read().unwrap().is_dirty;
                if is_dirty {
                    self.flush_page(evicted_page_id);
                }
                self.page_table.remove(&evicted_page_id);
                frame_id
            } else {
                return Err(BustubxError::Storage("Failed to evict page".to_string()));
            }
        };

        // 从磁盘分配一个页
        let new_page_id = self.disk_manager.allocate_page().unwrap();
        self.page_table.insert(new_page_id, frame_id);
        let mut new_page = Page::new(new_page_id);
        new_page.pin_count = 1;
        self.pool[frame_id as usize] = Arc::new(RwLock::new(new_page));

        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Ok(self.pool[frame_id as usize].clone())
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> BustubxResult<Arc<RwLock<Page>>> {
        if self.page_table.contains_key(&page_id) {
            let frame_id = self.page_table[&page_id];
            let page = self.pool[frame_id as usize].clone();
            page.write().unwrap().pin_count += 1;
            self.replacer.set_evictable(frame_id, false);
            return Ok(page);
        } else {
            // 分配一个frame
            let frame_id = if !self.free_list.is_empty() {
                self.free_list.pop_front().unwrap()
            } else {
                if let Some(frame_id) = self.replacer.evict() {
                    let evicted_page = self.pool[frame_id as usize].clone();
                    let evicted_page_id = evicted_page.read().unwrap().page_id;
                    if evicted_page.read().unwrap().is_dirty {
                        self.flush_page(evicted_page_id);
                    }
                    self.page_table.remove(&evicted_page_id);
                    frame_id
                } else {
                    return Err(BustubxError::Storage("Failed to evict page".to_string()));
                }
            };
            // 从磁盘读取页
            self.page_table.insert(page_id, frame_id);
            let mut new_page = Page::new(page_id);
            new_page.pin_count = 1;
            new_page.data = self.disk_manager.read_page(page_id).unwrap();
            self.pool[frame_id as usize] = Arc::new(RwLock::new(new_page));

            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);

            Ok(self.pool[frame_id as usize].clone())
        }
    }

    pub fn write_page(&mut self, page_id: PageId, data: [u8; BUSTUBX_PAGE_SIZE]) {
        if self.page_table.contains_key(&page_id) {
            let frame_id = self.page_table[&page_id];
            let page = self.pool[frame_id as usize].clone();
            page.write().unwrap().data = data;
            page.write().unwrap().is_dirty = true;
        }
    }

    // 从缓冲池中取消固定页
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> bool {
        if self.page_table.contains_key(&page_id) {
            let frame_id = self.page_table[&page_id];
            let page = self.pool[frame_id as usize].clone();
            if page.read().unwrap().pin_count == 0 {
                return false;
            }
            page.write().unwrap().pin_count -= 1;
            page.write().unwrap().is_dirty |= is_dirty;
            if page.read().unwrap().pin_count == 0 {
                self.replacer.set_evictable(frame_id, true);
            }
            true
        } else {
            false
        }
    }

    // 将缓冲池中指定页写回磁盘
    pub fn flush_page(&mut self, page_id: PageId) -> bool {
        if self.page_table.contains_key(&page_id) {
            let frame_id = self.page_table[&page_id];
            let page = self.pool[frame_id as usize].clone();
            self.disk_manager
                .write_page(page_id, &page.read().unwrap().data)
                .unwrap();
            page.write().unwrap().is_dirty = false;
            true
        } else {
            false
        }
    }

    // 将缓冲池中的所有页写回磁盘
    pub fn flush_all_pages(&mut self) {
        let page_ids: Vec<PageId> = self.page_table.keys().into_iter().copied().collect();
        for page_id in page_ids {
            self.flush_page(page_id);
        }
    }

    // 删除缓冲池中的页
    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        if !self.page_table.contains_key(&page_id) {
            return true;
        }
        let frame_id = self.page_table[&page_id];
        let page = self.pool[frame_id as usize].clone();
        if page.read().unwrap().pin_count > 0 {
            // 页被固定，无法删除
            return false;
        }

        // 从缓冲池中删除
        page.write().unwrap().destroy();
        self.page_table.remove(&page_id);
        self.free_list.push_back(frame_id);
        self.replacer.remove(frame_id);

        // 从磁盘上删除
        self.disk_manager.deallocate_page(page_id).unwrap();
        return true;
    }
}

#[cfg(test)]
mod tests {
    use crate::{buffer::BufferPoolManager, storage::DiskManager};
    use std::{fs::remove_file, sync::Arc};

    #[test]
    pub fn test_buffer_pool_manager_new_page() {
        let db_path = "./test_buffer_pool_manager_new_page.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let mut buffer_pool_manager = BufferPoolManager::new(3, Arc::new(disk_manager));
        let page = buffer_pool_manager.new_page().unwrap().clone();
        assert_eq!(page.read().unwrap().page_id, 0);
        assert_eq!(
            buffer_pool_manager.pool[0].read().unwrap().page_id,
            page.read().unwrap().page_id
        );
        assert_eq!(
            buffer_pool_manager.page_table[&page.read().unwrap().page_id],
            0
        );
        assert_eq!(buffer_pool_manager.free_list.len(), 2);
        assert_eq!(buffer_pool_manager.replacer.size(), 0);

        let page = buffer_pool_manager.new_page().unwrap();
        assert_eq!(page.read().unwrap().page_id, 1);
        let page = buffer_pool_manager.new_page().unwrap();
        assert_eq!(page.read().unwrap().page_id, 2);
        let page = buffer_pool_manager.new_page();
        assert!(page.is_err());

        buffer_pool_manager.unpin_page(0, false);
        let page = buffer_pool_manager.new_page().unwrap();
        assert_eq!(page.read().unwrap().page_id, 3);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_buffer_pool_manager_unpin_page() {
        let db_path = "./test_buffer_pool_manager_unpin_page.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let mut buffer_pool_manager = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page = buffer_pool_manager.new_page().unwrap();
        let page = buffer_pool_manager.new_page().unwrap();
        let page = buffer_pool_manager.new_page().unwrap();
        let page = buffer_pool_manager.new_page();
        assert!(page.is_err());

        buffer_pool_manager.unpin_page(0, true);
        let page = buffer_pool_manager.new_page().unwrap();
        assert_eq!(page.read().unwrap().page_id, 3);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_buffer_pool_manager_fetch_page() {
        let db_path = "./test_buffer_pool_manager_fetch_page.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let mut buffer_pool_manager = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(0, true);
        let page = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(1, false);
        let page = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(2, false);

        let page = buffer_pool_manager.fetch_page(0).unwrap();
        assert_eq!(page.read().unwrap().page_id, 0);
        buffer_pool_manager.unpin_page(0, false);

        let page = buffer_pool_manager.fetch_page(1).unwrap();
        assert_eq!(page.read().unwrap().page_id, 1);
        buffer_pool_manager.unpin_page(1, false);
        assert_eq!(buffer_pool_manager.replacer.size(), 3);

        let _ = remove_file(db_path);
    }

    #[test]
    pub fn test_buffer_pool_manager_delete_page() {
        let db_path = "./test_buffer_pool_manager_delete_page.db";
        let _ = remove_file(db_path);

        let disk_manager = DiskManager::try_new(&db_path).unwrap();
        let mut buffer_pool_manager = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page_id = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(0, true);
        let page_id = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(1, true);
        let page_id = buffer_pool_manager.new_page().unwrap();
        buffer_pool_manager.unpin_page(2, false);

        let res = buffer_pool_manager.delete_page(0);
        assert!(res);
        assert_eq!(buffer_pool_manager.pool.len(), 3);
        assert_eq!(buffer_pool_manager.free_list.len(), 1);
        assert_eq!(buffer_pool_manager.replacer.size(), 2);
        assert_eq!(buffer_pool_manager.page_table.len(), 2);

        let page = buffer_pool_manager.fetch_page(1).unwrap();
        assert_eq!(page.read().unwrap().page_id, 1);

        let _ = remove_file(db_path);
    }
}
