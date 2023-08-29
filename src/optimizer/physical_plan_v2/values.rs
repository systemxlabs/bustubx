use std::sync::atomic::AtomicU32;

use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
    execution::{ExecutionContext, VolcanoExecutorV2},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub struct PhysicalValues {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,

    cursor: AtomicU32,
}
impl PhysicalValues {
    pub fn new(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> Self {
        PhysicalValues {
            columns,
            tuples,
            cursor: AtomicU32::new(0),
        }
    }
    pub fn output_schema(&self) -> Schema {
        return Schema::new(self.columns.clone());
    }
}
impl VolcanoExecutorV2 for PhysicalValues {
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
