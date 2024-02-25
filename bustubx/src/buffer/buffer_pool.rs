use dashmap::DashMap;
use std::sync::RwLock;
use std::{collections::VecDeque, sync::Arc};

use crate::buffer::page::{Page, PageId};
use crate::buffer::BUSTUBX_PAGE_SIZE;
use crate::storage::DiskManager;
use crate::{BustubxError, BustubxResult};

use super::replacer::LRUKReplacer;

pub type FrameId = usize;

pub const BUFFER_POOL_SIZE: usize = 1000;

#[derive(Debug)]
pub struct BufferPoolManager {
    pool: Vec<Arc<RwLock<Page>>>,
    // LRU-K置换算法
    pub replacer: Arc<RwLock<LRUKReplacer>>,
    pub disk_manager: Arc<DiskManager>,
    // 缓冲池中的页号与frame号的映射
    page_table: DashMap<PageId, FrameId>,
    // 缓冲池中空闲的frame
    free_list: Arc<RwLock<VecDeque<FrameId>>>,
}
impl BufferPoolManager {
    pub fn new(num_pages: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut free_list = VecDeque::with_capacity(num_pages);
        let mut pool = vec![];
        for i in 0..num_pages {
            free_list.push_back(i);
            pool.push(Arc::new(RwLock::new(Page::empty())));
        }

        Self {
            pool,
            replacer: Arc::new(RwLock::new(LRUKReplacer::new(num_pages, 2))),
            disk_manager,
            page_table: DashMap::new(),
            free_list: Arc::new(RwLock::new(free_list)),
        }
    }

    // 从缓冲池创建一个新页
    pub fn new_page(&self) -> BustubxResult<Arc<RwLock<Page>>> {
        // 缓冲池已满且无可替换的页
        if self.free_list.read().unwrap().is_empty() && self.replacer.read().unwrap().size() == 0 {
            return Err(BustubxError::Storage(
                "Cannot new page because buffer pool is full and no page to evict".to_string(),
            ));
        }

        // 分配一个frame
        let frame_id = self.allocate_frame()?;

        // 从磁盘分配一个页
        let new_page_id = self.disk_manager.allocate_page().unwrap();
        self.page_table.insert(new_page_id, frame_id);
        let mut new_page = Page::new(new_page_id);
        new_page.pin_count = 1;
        self.pool[frame_id].write().unwrap().replace(new_page);

        self.replacer.write().unwrap().record_access(frame_id)?;
        self.replacer
            .write()
            .unwrap()
            .set_evictable(frame_id, false)?;

        Ok(self.pool[frame_id].clone())
    }

    pub fn fetch_page(&self, page_id: PageId) -> BustubxResult<Arc<RwLock<Page>>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = self.pool[*frame_id].clone();
            page.write().unwrap().pin_count += 1;
            self.replacer
                .write()
                .unwrap()
                .set_evictable(*frame_id, false)?;
            Ok(page)
        } else {
            // 分配一个frame
            let frame_id = self.allocate_frame()?;

            // 从磁盘读取页
            self.page_table.insert(page_id, frame_id);
            let new_page = Page::new(page_id)
                .with_pin_count(1u32)
                .with_data(self.disk_manager.read_page(page_id)?);
            self.pool[frame_id].write().unwrap().replace(new_page);

            self.replacer.write().unwrap().record_access(frame_id)?;
            self.replacer
                .write()
                .unwrap()
                .set_evictable(frame_id, false)?;

            Ok(self.pool[frame_id].clone())
        }
    }

    // 从缓冲池中取消固定页
    pub fn unpin_page_id(&self, page_id: PageId, is_dirty: bool) -> BustubxResult<()> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = self.pool[*frame_id].clone();
            if page.read().unwrap().pin_count == 0 {
                return Ok(());
            }
            page.write().unwrap().pin_count -= 1;
            page.write().unwrap().is_dirty |= is_dirty;
            let pin_count = page.read().unwrap().pin_count;
            if pin_count == 0 {
                self.replacer
                    .write()
                    .unwrap()
                    .set_evictable(*frame_id, true)?;
            }
            Ok(())
        } else {
            Err(BustubxError::Storage(format!(
                "Cannot unpin page id {} as it is not in the pool",
                page_id
            )))
        }
    }

    pub fn unpin_page(&self, page: Arc<RwLock<Page>>, is_dirty: bool) -> BustubxResult<()> {
        let page_id = page.read().unwrap().page_id;
        self.unpin_page_id(page_id, is_dirty)
    }

    // 将缓冲池中指定页写回磁盘
    pub fn flush_page(&self, page_id: PageId) -> BustubxResult<bool> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = self.pool[*frame_id].clone();
            self.disk_manager
                .write_page(page_id, page.read().unwrap().data())?;
            page.write().unwrap().is_dirty = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // 将缓冲池中的所有页写回磁盘
    pub fn flush_all_pages(&self) -> BustubxResult<()> {
        let page_ids: Vec<PageId> = self.page_table.iter().map(|e| *e.key()).collect();
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    // 删除缓冲池中的页
    pub fn delete_page(&self, page_id: PageId) -> BustubxResult<bool> {
        if let Some(frame_id_lock) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_lock;
            drop(frame_id_lock);

            let page = self.pool[frame_id].clone();
            if page.read().unwrap().pin_count > 0 {
                // 页被固定，无法删除
                return Ok(false);
            }

            // 从缓冲池中删除
            page.write().unwrap().destroy();
            self.page_table.remove(&page_id);
            self.free_list.write().unwrap().push_back(frame_id);
            self.replacer.write().unwrap().remove(frame_id);

            // 从磁盘上删除
            self.disk_manager.deallocate_page(page_id)?;
            Ok(true)
        } else {
            Ok(true)
        }
    }

    fn allocate_frame(&self) -> BustubxResult<FrameId> {
        if let Some(frame_id) = self.free_list.write().unwrap().pop_front() {
            Ok(frame_id)
        } else if let Some(frame_id) = self.replacer.write().unwrap().evict() {
            let evicted_page = self.pool[frame_id].clone();
            let evicted_page_id = evicted_page.read().unwrap().page_id;
            let is_dirty = evicted_page.read().unwrap().is_dirty;
            if is_dirty {
                self.flush_page(evicted_page_id)?;
            }
            self.page_table.remove(&evicted_page_id);
            Ok(frame_id)
        } else {
            Err(BustubxError::Storage(
                "Cannot allocate free frame".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{buffer::BufferPoolManager, storage::DiskManager};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    pub fn test_buffer_pool_manager_new_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = BufferPoolManager::new(3, Arc::new(disk_manager));
        let page1 = buffer_pool.new_page().unwrap().clone();
        let page1_id = page1.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[0].read().unwrap().page_id, page1_id,);
        assert_eq!(
            *buffer_pool
                .page_table
                .get(&page1.read().unwrap().page_id)
                .unwrap(),
            0
        );
        assert_eq!(buffer_pool.free_list.read().unwrap().len(), 2);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 0);

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[1].read().unwrap().page_id, page2_id,);

        let page3 = buffer_pool.new_page().unwrap();
        let page3_id = page3.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[2].read().unwrap().page_id, page3_id,);

        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        buffer_pool.unpin_page_id(page1_id, false).unwrap();
        let page5 = buffer_pool.new_page().unwrap();
        let page5_id = page5.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[0].read().unwrap().page_id, page5_id,);
    }

    #[test]
    pub fn test_buffer_pool_manager_unpin_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        let _page2 = buffer_pool.new_page().unwrap();
        let _page3 = buffer_pool.new_page().unwrap();
        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        buffer_pool.unpin_page_id(page1_id, true).unwrap();
        let page5 = buffer_pool.new_page();
        assert!(page5.is_ok());
    }

    #[test]
    pub fn test_buffer_pool_manager_fetch_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page1_id, true).unwrap();

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page2_id, false).unwrap();

        let page3 = buffer_pool.new_page().unwrap();
        let page3_id = page3.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page3_id, false).unwrap();

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page1_id);
        buffer_pool.unpin_page_id(page1_id, false).unwrap();

        let page = buffer_pool.fetch_page(page2_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page2_id);
        buffer_pool.unpin_page_id(page2_id, false).unwrap();

        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 3);
    }

    #[test]
    pub fn test_buffer_pool_manager_delete_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let buffer_pool = BufferPoolManager::new(3, Arc::new(disk_manager));

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page1_id, true).unwrap();

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page2_id, true).unwrap();

        let page3 = buffer_pool.new_page().unwrap();
        let page3_id = page3.read().unwrap().page_id;
        buffer_pool.unpin_page_id(page3_id, false).unwrap();

        let res = buffer_pool.delete_page(page1_id).unwrap();
        assert!(res);
        assert_eq!(buffer_pool.pool.len(), 3);
        assert_eq!(buffer_pool.free_list.read().unwrap().len(), 1);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 2);
        assert_eq!(buffer_pool.page_table.len(), 2);

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page1_id);
    }
}
