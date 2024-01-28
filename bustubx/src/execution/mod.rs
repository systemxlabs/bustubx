use std::sync::Arc;

use tracing::span;

use crate::{
    catalog::{catalog::Catalog, Schema},
    planner::physical_plan::PhysicalPlan,
    storage::tuple::Tuple,
};

pub trait VolcanoExecutor {
    fn init(&self, context: &mut ExecutionContext);
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple>;
}

#[derive(derive_new::new)]
pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl ExecutionEngine<'_> {
    pub fn execute(&mut self, plan: Arc<PhysicalPlan>) -> (Vec<Tuple>, Schema) {
        let _execute_span = span!(tracing::Level::INFO, "executionengine.execute").entered();
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
