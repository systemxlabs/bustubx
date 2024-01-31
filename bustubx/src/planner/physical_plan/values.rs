use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::catalog::{ColumnRef, SchemaRef};
use crate::expression::{Expr, ExprTrait};
use crate::{
    catalog::Schema,
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
};

#[derive(Debug)]
pub struct PhysicalValues {
    pub columns: Vec<ColumnRef>,
    pub tuples: Vec<Vec<Expr>>,

    cursor: AtomicU32,
}
impl PhysicalValues {
    pub fn new(columns: Vec<ColumnRef>, tuples: Vec<Vec<Expr>>) -> Self {
        PhysicalValues {
            columns,
            tuples,
            cursor: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalValues {
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        let cursor = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
        return if cursor < self.tuples.len() {
            let values = self.tuples[cursor].clone();
            Some(Tuple::new(
                self.output_schema(),
                values
                    .into_iter()
                    .map(|v| {
                        v.evaluate(&Tuple::empty(Arc::new(Schema::empty())))
                            .unwrap()
                    })
                    .collect(),
            ))
        } else {
            None
        };
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema {
            columns: self.columns.clone(),
        })
    }
}

impl std::fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
