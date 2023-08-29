use std::sync::Arc;

use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub predicate: BoundExpression,
    pub input: Arc<PhysicalOperator>,
}
impl PhysicalFilter {
    pub fn new(predicate: BoundExpression, input: Arc<PhysicalOperator>) -> Self {
        PhysicalFilter { predicate, input }
    }
    pub fn output_schema(&self) -> Schema {
        self.input.output_schema()
    }
}
