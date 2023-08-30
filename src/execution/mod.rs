use std::sync::Arc;

use crate::{
    catalog::{catalog::Catalog, schema::Schema},
    optimizer::physical_plan::PhysicalPlan,
    storage::tuple::Tuple,
};

pub trait VolcanoExecutor {
    fn init(&self, context: &mut ExecutionContext);
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple>;
}

pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
}
impl ExecutionContext<'_> {
    pub fn new(catalog: &mut Catalog) -> ExecutionContext {
        ExecutionContext { catalog }
    }
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl ExecutionEngine<'_> {
    pub fn execute(&mut self, plan: Arc<PhysicalPlan>) -> (Vec<Tuple>, Schema) {
        plan.init(&mut self.context);
        let mut result = Vec::new();
        loop {
            let next_tuple = plan.next(&mut self.context);
            if next_tuple.is_some() {
                result.push(next_tuple.unwrap());
            } else {
                break;
            }
        }
        let schema = plan.output_schema();
        (result, schema)
    }
}
