use std::sync::atomic::AtomicU32;

use crate::catalog::column::ColumnRef;
use crate::{
    catalog::{column::Column, schema::Schema},
    common::scalar::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub struct PhysicalValues {
    pub columns: Vec<ColumnRef>,
    pub tuples: Vec<Vec<ScalarValue>>,

    cursor: AtomicU32,
}
impl PhysicalValues {
    pub fn new(columns: Vec<ColumnRef>, tuples: Vec<Vec<ScalarValue>>) -> Self {
        PhysicalValues {
            columns,
            tuples,
            cursor: AtomicU32::new(0),
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema {
            columns: self.columns.clone(),
        }
    }
}
impl VolcanoExecutor for PhysicalValues {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init values executor");
        self.cursor.store(0, std::sync::atomic::Ordering::SeqCst);
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        let cursor = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
        if cursor < self.tuples.len() {
            let values = self.tuples[cursor].clone();
            return Some(Tuple::from_values(values));
        } else {
            return None;
        }
    }
}
