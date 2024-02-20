use log::debug;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalProject {
    pub exprs: Vec<Expr>,
    pub schema: SchemaRef,
    pub input: Arc<PhysicalPlan>,
}

impl VolcanoExecutor for PhysicalProject {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init project executor");
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let next_tuple = self.input.next(context)?;
        if next_tuple.is_none() {
            return Ok(None);
        }
        let next_tuple = next_tuple.unwrap();
        let mut new_values = Vec::new();
        for expr in &self.exprs {
            new_values.push(expr.evaluate(&next_tuple)?);
        }
        return Ok(Some(Tuple::new(self.output_schema(), new_values)));
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Project")
    }
}
