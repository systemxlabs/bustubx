mod common;
mod index_page;
mod meta_page;
mod scalar;
mod table_page;
mod tuple;

pub use common::CommonCodec;
pub use index_page::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec, BPlusTreePageTypeCodec,
};
pub use meta_page::MetaPageCodec;
pub use scalar::ScalarValueCodec;
pub use table_page::{
    RidCodec, TablePageCodec, TablePageHeaderCodec, TablePageHeaderTupleInfoCodec,
};
pub use tuple::TupleCodec;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
