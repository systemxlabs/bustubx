use crate::catalog::{SchemaRef, UPDATE_OUTPUT_SCHEMA_REF};
use crate::common::TableReference;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::Expr;
use crate::{BustubxResult, Tuple};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug)]
pub struct PhysicalUpdate {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub assignments: HashMap<String, Expr>,
    pub selection: Option<Expr>,

    update_rows: AtomicU32,
}

impl PhysicalUpdate {
    pub fn new(
        table: TableReference,
        table_schema: SchemaRef,
        assignments: HashMap<String, Expr>,
        selection: Option<Expr>,
    ) -> Self {
        Self {
            table,
            table_schema,
            assignments,
            selection,
            update_rows: AtomicU32::new(0),
        }
    }
}

impl VolcanoExecutor for PhysicalUpdate {
    fn init(&self, _context: &mut ExecutionContext) -> BustubxResult<()> {
        self.update_rows.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        // TODO implementation
        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        UPDATE_OUTPUT_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Update")
    }
}
