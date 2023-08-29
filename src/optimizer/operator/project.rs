use crate::{binder::expression::BoundExpression, catalog::schema::Schema};

#[derive(Debug)]
pub struct PhysicalProject {
    pub expressions: Vec<BoundExpression>,
}
impl PhysicalProject {
    pub fn new(expressions: Vec<BoundExpression>) -> Self {
        PhysicalProject { expressions }
    }
    pub fn output_schema(&self) -> Schema {
        unimplemented!()
    }
}
