use std::sync::Arc;

use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalFilterOperator {
    pub predicate: BoundExpression,
    pub input: Arc<PhysicalOperator>,
}
impl PhysicalFilterOperator {
    pub fn new(predicate: BoundExpression, input: Arc<PhysicalOperator>) -> Self {
        PhysicalFilterOperator { predicate, input }
    }
    pub fn output_schema(&self) -> Schema {
        self.input.output_schema()
    }
}
