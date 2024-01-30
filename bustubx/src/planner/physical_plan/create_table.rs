use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
};
use std::sync::Arc;

#[derive(derive_new::new, Debug)]
pub struct PhysicalCreateTable {
    pub table_name: String,
    pub schema: Schema,
}
impl PhysicalCreateTable {
    pub fn output_schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }
}
impl VolcanoExecutor for PhysicalCreateTable {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init create table executor");
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        context
            .catalog
            .create_table(self.table_name.clone(), Arc::new(self.schema.clone()));
        None
    }
}
