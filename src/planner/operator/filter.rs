use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

#[derive(Debug)]
pub struct LogicalFilterOperator {
    pub predicate: BoundExpression,
}
impl LogicalFilterOperator {
    pub fn new(predicate: BoundExpression) -> Self {
        Self { predicate }
    }
    pub fn output_schema(&self) -> Schema {
        unimplemented!()
    }
}
