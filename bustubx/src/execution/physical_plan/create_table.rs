use crate::catalog::SchemaRef;
use crate::common::table_ref::TableReference;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};
use std::sync::Arc;

#[derive(derive_new::new, Debug)]
pub struct PhysicalCreateTable {
    pub table: TableReference,
    pub schema: Schema,
}

impl VolcanoExecutor for PhysicalCreateTable {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        context.catalog.create_table(
            self.table.table().to_string(),
            Arc::new(self.schema.clone()),
        );
        Ok(None)
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }
}

impl std::fmt::Display for PhysicalCreateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
