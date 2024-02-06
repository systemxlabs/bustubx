mod common;
mod tuple;

pub use common::CommonCodec;
pub use tuple::TupleCodec;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
