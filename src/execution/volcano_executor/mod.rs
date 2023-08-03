use crate::storage::tuple::Tuple;

pub mod insert;
pub mod values;

pub trait VolcanoExecutor {
    fn init(&mut self);
    fn next(&mut self) -> Option<Tuple>;
}
