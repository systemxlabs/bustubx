use crate::catalog::{SchemaRef, UPDATE_OUTPUT_SCHEMA_REF};
use crate::common::{ScalarValue, TableReference};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::{Expr, ExprTrait};
use crate::storage::{TableIterator, EMPTY_TUPLE};
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
        // TODO may scan index
        let table_heap = context.catalog.table_heap(&self.table)?;
        let mut table_iterator = TableIterator::new(table_heap.clone(), ..);
        loop {
            if let Some((rid, mut tuple)) = table_iterator.next()? {
                if let Some(selection) = &self.selection {
                    if !selection.evaluate(&tuple)?.as_boolean()?.unwrap_or(false) {
                        continue;
                    }
                }
                // update tuple data
                for (col_name, value_expr) in self.assignments.iter() {
                    let new_value = value_expr.evaluate(&EMPTY_TUPLE)?;
                    let index = tuple.schema.index_of(None, &col_name)?;
                    tuple.data[index] = new_value;
                }
                table_heap.update_tuple(rid, tuple)?;
                self.update_rows.fetch_add(1, Ordering::SeqCst);
            } else {
                return if self.update_rows.load(Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    let update_rows = self.update_rows.swap(0, Ordering::SeqCst);
                    Ok(Some(Tuple::new(
                        self.output_schema(),
                        vec![ScalarValue::Int32(Some(update_rows as i32))],
                    )))
                };
            }
        }
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
