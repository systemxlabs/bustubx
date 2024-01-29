use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub struct PhysicalCreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub key_attrs: Vec<u32>,
}
impl PhysicalCreateIndex {
    pub fn new(
        index_name: String,
        table_name: String,
        table_schema: SchemaRef,
        key_attrs: Vec<u32>,
    ) -> Self {
        Self {
            index_name,
            table_name,
            table_schema,
            key_attrs,
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::copy_schema(&self.table_schema, &self.key_attrs)
    }
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
}
