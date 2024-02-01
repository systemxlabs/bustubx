use crate::catalog::{Schema, SchemaRef};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::{BustubxResult, Tuple};
use std::sync::Arc;

#[derive(Debug)]
pub struct Empty {
    pub schema: SchemaRef,
}

impl VolcanoExecutor for Empty {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for Empty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Empty")
    }
}
