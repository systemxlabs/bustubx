use std::sync::Arc;

use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    planner::expr::Expr,
    storage::Tuple,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalProject {
    pub expressions: Vec<Expr>,
    pub input: Arc<PhysicalPlan>,
}
impl PhysicalProject {
    pub fn output_schema(&self) -> Schema {
        // TODO consider aggr/alias
        self.input.output_schema()
    }
}
impl VolcanoExecutor for PhysicalProject {
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
        return Some(Tuple::new(Arc::new(self.output_schema()), new_values));
    }
}
