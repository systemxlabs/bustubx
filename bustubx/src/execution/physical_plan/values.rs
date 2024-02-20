use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

#[derive(Debug)]
pub struct PhysicalValues {
    pub schema: SchemaRef,
    pub values: Vec<Vec<Expr>>,

    cursor: AtomicU32,
}
impl PhysicalValues {
    pub fn new(schema: SchemaRef, values: Vec<Vec<Expr>>) -> Self {
        PhysicalValues {
            schema,
            values,
            cursor: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalValues {
    fn next(&self, _context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let cursor = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
        if cursor < self.values.len() {
            let values = self.values[cursor].clone();
            Ok(Some(Tuple::new(
                self.output_schema(),
                values
                    .into_iter()
                    .map(|v| {
                        v.evaluate(&Tuple::empty(Arc::new(Schema::empty())))
                            .unwrap()
                    })
                    .collect(),
            )))
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Values")
    }
}
