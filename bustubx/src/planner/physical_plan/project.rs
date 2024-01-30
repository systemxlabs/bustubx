use std::sync::Arc;

use crate::catalog::SchemaRef;
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
        return Some(Tuple::new(self.output_schema(), new_values));
    }

    fn output_schema(&self) -> SchemaRef {
        // TODO consider aggr/alias
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
