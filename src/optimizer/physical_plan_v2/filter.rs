use std::sync::Arc;

use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

use super::PhysicalPlanV2;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub predicate: BoundExpression,
    pub input: Arc<PhysicalPlanV2>,
}
impl PhysicalFilter {
    pub fn new(predicate: BoundExpression, input: Arc<PhysicalPlanV2>) -> Self {
        PhysicalFilter { predicate, input }
    }
    pub fn output_schema(&self) -> Schema {
        self.input.output_schema()
    }
}
