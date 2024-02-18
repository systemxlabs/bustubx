use std::fs::File;
use std::path::Path;
use std::{
    io::{Read, Seek, Write},
    sync::{atomic::AtomicU32, Mutex, MutexGuard},
};
use tracing::info;

use crate::error::{BustubxError, BustubxResult};

use crate::buffer::{PageId, BUSTUBX_PAGE_SIZE};
use crate::storage::codec::MetaPageCodec;
use crate::storage::meta_page::MetaPage;
use crate::storage::{EMPTY_META_PAGE, META_PAGE_SIZE};

static EMPTY_PAGE: [u8; BUSTUBX_PAGE_SIZE] = [0; BUSTUBX_PAGE_SIZE];

#[derive(Debug)]
pub struct DiskManager {
    next_page_id: AtomicU32,
    db_file: Mutex<File>,
    meta: MetaPage,
}

impl DiskManager {
    pub fn try_new(db_path: impl AsRef<Path>) -> BustubxResult<Self> {
        let (db_file, meta) = if db_path.as_ref().exists() {
            let mut db_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(db_path)?;
            let mut buf = vec![0; *META_PAGE_SIZE];
            db_file.read_exact(&mut buf)?;
            let (meta_page, _) = MetaPageCodec::decode(&buf)?;
            (db_file, meta_page)
        } else {
            let mut db_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path)?;
            let meta_page = MetaPage::try_new()?;
            db_file.write(&MetaPageCodec::encode(&meta_page))?;
            (db_file, meta_page)
        };

        // calculate next page id
        let db_file_len = db_file.metadata()?.len();
        if (db_file_len - *META_PAGE_SIZE as u64) % BUSTUBX_PAGE_SIZE as u64 != 0 {
            return Err(BustubxError::Internal(format!(
                "db file size not a multiple of {} + meta page size {}",
                BUSTUBX_PAGE_SIZE, *META_PAGE_SIZE,
            )));
        }
        let next_page_id =
            (((db_file_len - *META_PAGE_SIZE as u64) / BUSTUBX_PAGE_SIZE as u64) + 1) as PageId;
        info!("Initialized disk_manager next_page_id: {}", next_page_id);

        Ok(Self {
            next_page_id: AtomicU32::new(next_page_id),
            // Use a mutex to wrap the file handle to ensure that only one thread
            // can access the file at the same time among multiple threads.
            db_file: Mutex::new(db_file),
            meta,
        })
    }

    pub fn read_page(&self, page_id: PageId) -> BustubxResult<[u8; BUSTUBX_PAGE_SIZE]> {
        let mut guard = self.db_file.lock().unwrap();
        let mut buf = [0; BUSTUBX_PAGE_SIZE];

        // set offset and read page data
        guard.seek(std::io::SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * BUSTUBX_PAGE_SIZE) as u64,
        ))?;
        // Read buf.len() bytes of data from the file, and store the data in the buf array.
        guard.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> BustubxResult<()> {
        if data.len() != BUSTUBX_PAGE_SIZE {
            return Err(BustubxError::Storage(format!(
                "Page size is not {}",
                BUSTUBX_PAGE_SIZE
            )));
        }
        let mut guard = self.db_file.lock().unwrap();
        Self::write_page_internal(&mut guard, page_id, data)
    }

    // TODO 使用bitmap管理
    pub fn allocate_page(&self) -> BustubxResult<PageId> {
        let mut guard = self.db_file.lock().unwrap();

        // fetch current value and increment page id
        let page_id = self
            .next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Write an empty page (all zeros) to the allocated page.
        Self::write_page_internal(&mut guard, page_id, &EMPTY_PAGE)?;

        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> BustubxResult<()> {
        // TODO 利用pageId或者释放的空间
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
        guard.seek(std::io::SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * BUSTUBX_PAGE_SIZE) as u64,
        ))?;
        guard.write_all(data)?;
        guard.flush()?;
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
    use crate::buffer::BUSTUBX_PAGE_SIZE;
    use crate::storage::codec::MetaPageCodec;
    use crate::storage::EMPTY_META_PAGE;
    use tempfile::TempDir;

    #[test]
    pub fn test_disk_manager_write_read_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = super::DiskManager::try_new(&temp_path).unwrap();

        let page_id1 = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id1, 1);
        let mut page1 = vec![1, 2, 3];
        page1.extend(vec![0; BUSTUBX_PAGE_SIZE - 3]);
        disk_manager.write_page(page_id1, &page1).unwrap();
        let page = disk_manager.read_page(page_id1).unwrap();
        assert_eq!(page, page1.as_slice());

        let page_id2 = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id2, 2);
        let mut page2 = vec![0; BUSTUBX_PAGE_SIZE - 3];
        page2.extend(vec![4, 5, 6]);
        disk_manager.write_page(page_id2, &page2).unwrap();
        let page = disk_manager.read_page(page_id2).unwrap();
        assert_eq!(page, page2.as_slice());

        let db_file_len = disk_manager.db_file_len().unwrap();
        assert_eq!(
            db_file_len as usize,
            BUSTUBX_PAGE_SIZE * 2 + MetaPageCodec::encode(&EMPTY_META_PAGE).len()
        );
    }
}
