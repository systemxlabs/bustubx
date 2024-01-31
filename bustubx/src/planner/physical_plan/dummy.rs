use crate::catalog::{Schema, SchemaRef};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::{BustubxResult, Tuple};
use std::sync::Arc;

#[derive(Debug)]
pub struct Dummy;

impl VolcanoExecutor for Dummy {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

impl std::fmt::Display for Dummy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dummy")
    }
}
