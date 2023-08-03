use crate::{catalog::column::Column, dbtype::value::Value, storage::tuple::Tuple};

use super::VolcanoExecutor;

pub struct ValuesExecutor {
    cursor: usize,
    columns: Vec<Column>,
    tuples: Vec<Vec<Value>>,
}
impl VolcanoExecutor for ValuesExecutor {
    fn init(&mut self) {
        self.cursor = 0;
    }
    fn next(&mut self) -> Option<Tuple> {
        if self.cursor < self.tuples.len() {
            let tuple = self.tuples[self.cursor].clone();
            self.cursor += 1;
            // Some(tuple)
            // TODO 构建tuple
            unimplemented!()
        } else {
            None
        }
    }
}
