use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
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
    fn init(&self, context: &mut ExecutionContext) {
        println!("init create index executor");
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        context.catalog.create_index(
            self.index_name.clone(),
            self.table_name.clone(),
            self.key_attrs.clone(),
        );
        None
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::copy_schema(
            self.table_schema.clone(),
            &self.key_attrs,
        ))
    }
}
