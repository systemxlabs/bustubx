use std::sync::Arc;

use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

use super::PhysicalPlanV2;

#[derive(Debug)]
pub struct PhysicalProject {
    pub expressions: Vec<BoundExpression>,
    pub input: Arc<PhysicalPlanV2>,
}
impl PhysicalProject {
    pub fn new(expressions: Vec<BoundExpression>, input: Arc<PhysicalPlanV2>) -> Self {
        PhysicalProject { expressions, input }
    }
    pub fn output_schema(&self) -> Schema {
        unimplemented!()
    }
}
