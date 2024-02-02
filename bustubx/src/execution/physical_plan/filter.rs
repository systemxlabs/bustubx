use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::{
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalFilter {
    pub predicate: Expr,
    pub input: Arc<PhysicalPlan>,
}

impl VolcanoExecutor for PhysicalFilter {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        println!("init filter executor");
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        loop {
            let next_tuple = self.input.next(context)?;
            if next_tuple.is_none() {
                return Ok(None);
            }
            let tuple = next_tuple.unwrap();
            let compare_res = self.predicate.evaluate(&tuple)?;
            if let ScalarValue::Boolean(Some(v)) = compare_res {
                if v {
                    return Ok(Some(tuple));
                }
            } else {
                panic!("filter predicate should be boolean")
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
