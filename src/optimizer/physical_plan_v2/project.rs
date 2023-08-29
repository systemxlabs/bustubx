use std::sync::Arc;

use crate::{
    binder::expression::BoundExpression,
    catalog::schema::Schema,
    execution::{ExecutionContext, VolcanoExecutorV2},
    storage::tuple::Tuple,
};

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
impl VolcanoExecutorV2 for PhysicalProject {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init project executor");
        self.input.init(context);
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        let next_tuple = self.input.next(context);
        if next_tuple.is_none() {
            return None;
        }
        let mut new_values = Vec::new();
        for expr in &self.expressions {
            new_values.push(expr.evaluate(next_tuple.as_ref(), Some(&self.input.output_schema())));
        }
        return Some(Tuple::from_values(new_values));
    }
}
