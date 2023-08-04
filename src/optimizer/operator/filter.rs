use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

#[derive(Debug)]
pub struct PhysicalFilterOperator {
    pub predicate: BoundExpression,
}
impl PhysicalFilterOperator {
    pub fn new(predicate: BoundExpression) -> Self {
        PhysicalFilterOperator { predicate }
    }
    pub fn output_schema(&self) -> Schema {
        unimplemented!()
    }
}
