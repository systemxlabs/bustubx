mod common;
mod scalar;
mod table_page;
mod tuple;

pub use common::CommonCodec;
pub use scalar::ScalarValueCodec;
pub use table_page::{TablePageCodec, TablePageHeaderCodec, TablePageHeaderTupleInfoCodec};
pub use tuple::TupleCodec;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
