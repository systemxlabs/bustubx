use log::debug;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::{
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxError, BustubxResult,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalFilter {
    pub predicate: Expr,
    pub input: Arc<PhysicalPlan>,
}

impl VolcanoExecutor for PhysicalFilter {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init filter executor");
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        loop {
            if let Some(tuple) = self.input.next(context)? {
                let compare_res = self.predicate.evaluate(&tuple)?;
                if let ScalarValue::Boolean(Some(v)) = compare_res {
                    if v {
                        return Ok(Some(tuple));
                    }
                } else {
                    return Err(BustubxError::Execution(
                        "filter predicate value should be boolean".to_string(),
                    ));
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.predicate)
    }
}
