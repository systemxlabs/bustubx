pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::execution::physical_plan::PhysicalPlan;
use crate::{catalog::Catalog, storage::Tuple, BustubxResult};

pub trait VolcanoExecutor {
    fn init(&self, _context: &mut ExecutionContext) -> BustubxResult<()> {
        Ok(())
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>>;
    fn output_schema(&self) -> SchemaRef;
}

#[derive(derive_new::new)]
pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl ExecutionEngine<'_> {
    pub fn execute(&mut self, plan: Arc<PhysicalPlan>) -> BustubxResult<Vec<Tuple>> {
        plan.init(&mut self.context)?;
        let mut result = Vec::new();
        loop {
            let next_tuple = plan.next(&mut self.context)?;
            if let Some(tuple) = next_tuple {
                result.push(tuple);
            } else {
                break;
            }
        }
        Ok(result)
    }
}
