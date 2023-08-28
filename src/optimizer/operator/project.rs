use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

#[derive(Debug)]
pub struct PhysicalProjectOperator {
    pub expressions: Vec<BoundExpression>,
}
impl PhysicalProjectOperator {
    pub fn new(expressions: Vec<BoundExpression>) -> Self {
        PhysicalProjectOperator { expressions }
    }
    pub fn output_schema(&self) -> Schema {
        // TODO implementation
        Schema::new(vec![])
    }
}
