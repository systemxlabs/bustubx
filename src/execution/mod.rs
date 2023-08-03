use crate::optimizer::physical_plan::PhysicalPlan;

pub mod volcano_executor;

pub struct ExecutionEngine {}
impl ExecutionEngine {
    pub fn execute(&mut self, plan: PhysicalPlan) {
        unimplemented!()
    }
}
