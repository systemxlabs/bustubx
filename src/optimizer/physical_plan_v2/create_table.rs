use crate::{
    catalog::schema::Schema,
    execution::{ExecutionContext, VolcanoExecutorV2},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub struct PhysicalCreateTable {
    pub table_name: String,
    pub schema: Schema,
}
impl PhysicalCreateTable {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }
    pub fn output_schema(&self) -> Schema {
        self.schema.clone()
    }
}
impl VolcanoExecutorV2 for PhysicalCreateTable {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init create table executor");
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        context
            .catalog
            .create_table(self.table_name.clone(), self.schema.clone());
        None
    }
}
