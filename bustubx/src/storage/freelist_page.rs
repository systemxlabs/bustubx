use crate::buffer::{PageId, BUSTUBX_PAGE_SIZE, INVALID_PAGE_ID};
use crate::storage::codec::{CommonCodec, FreelistPageHeaderCodec};

static EMPTY_FREELIST_PAGE_HEADER: FreelistPageHeader = FreelistPageHeader {
    next_page_id: 0,
    current_size: 0,
    max_size: 0,
};

lazy_static::lazy_static! {
    pub static ref FREELIST_PAGE_MAX_SIZE: usize =
        (BUSTUBX_PAGE_SIZE - FreelistPageHeaderCodec::encode(&EMPTY_FREELIST_PAGE_HEADER).len())
            / CommonCodec::encode_u32(INVALID_PAGE_ID).len();
}

#[derive(Debug, Eq, PartialEq)]
pub struct FreelistPage {
    pub header: FreelistPageHeader,
    pub array: Vec<PageId>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct FreelistPageHeader {
    pub next_page_id: PageId,
    pub current_size: u32,
    pub max_size: u32,
}
