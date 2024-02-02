use crate::buffer::PageId;
use crate::catalog::{Column, DataType, Schema, SchemaRef};
use std::sync::Arc;

// 数据页的大小（字节）
pub const BUSTUBX_PAGE_SIZE: usize = 4096;

// table heap对应的缓冲池的大小（页）
pub const TABLE_HEAP_BUFFER_POOL_SIZE: usize = 100;

pub type TransactionId = u32;

lazy_static::lazy_static! {
    pub static ref EMPTY_SCHEMA_REF: SchemaRef = Arc::new(Schema::empty());

    pub static ref INSERT_OUTPUT_SCHEMA_REF: SchemaRef = Arc::new(Schema::new(
        vec![Column::new("insert_rows".to_string(), DataType::Int32)]
    ));
}
