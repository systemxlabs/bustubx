use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};
use std::sync::Arc;

#[derive(Debug, derive_new::new)]
pub struct PhysicalCreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub key_attrs: Vec<u32>,
}

impl VolcanoExecutor for PhysicalCreateIndex {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        context.catalog.create_index(
            self.index_name.clone(),
            self.table_name.clone(),
            self.key_attrs.clone(),
        );
        Ok(None)
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::copy_schema(
            self.table_schema.clone(),
            &self.key_attrs,
        ))
    }
}

impl std::fmt::Display for PhysicalCreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
