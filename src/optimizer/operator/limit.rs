use std::sync::Arc;

use crate::catalog::schema::Schema;

use super::PhysicalPlanV2;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub input: Arc<PhysicalPlanV2>,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: Option<usize>, input: Arc<PhysicalPlanV2>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
        }
    }
    pub fn output_schema(&self) -> Schema {
        return self.input.output_schema();
    }
}
