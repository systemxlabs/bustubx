use crate::catalog::SchemaRef;
use crate::Tuple;

pub struct TupleCodec;

impl TupleCodec {
    pub fn encode(tuple: &Tuple) -> Vec<u8> {
        todo!()
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> Tuple {
        todo!()
    }
}
