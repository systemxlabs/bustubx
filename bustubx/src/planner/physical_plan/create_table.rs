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

impl VolcanoExecutor for PhysicalCreateTable {
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        context
            .catalog
            .create_table(self.table_name.clone(), Arc::new(self.schema.clone()));
        None
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
