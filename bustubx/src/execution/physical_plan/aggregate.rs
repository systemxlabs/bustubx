use crate::catalog::SchemaRef;
use crate::common::ScalarValue;
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::{Accumulator, Expr, ExprTrait};
use crate::{BustubxError, BustubxResult, Tuple};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct PhysicalAggregate {
    /// The incoming physical plan
    pub input: Arc<PhysicalPlan>,
    /// Grouping expressions
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_exprs: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: SchemaRef,

    // TODO tmp solution
    pub output_count: AtomicUsize,
}

impl PhysicalAggregate {
    pub fn new(
        input: Arc<PhysicalPlan>,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            group_exprs,
            aggr_exprs,
            schema,
            output_count: AtomicUsize::new(0),
        }
    }
}

impl VolcanoExecutor for PhysicalAggregate {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        self.input.init(context)?;
        self.output_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        if self.output_count.load(Ordering::SeqCst) > 0 {
            return Ok(None);
        }

        // TODO support group
        let mut accumulators = self
            .aggr_exprs
            .iter()
            .map(|expr| {
                if let Expr::AggregateFunction(aggr) = expr {
                    Ok(aggr.func_kind.create_accumulator())
                } else {
                    Err(BustubxError::Execution(format!(
                        "aggr expr is not AggregateFunction instead of {}",
                        expr
                    )))
                }
            })
            .collect::<BustubxResult<Vec<Box<dyn Accumulator>>>>()?;

        loop {
            if let Some(tuple) = self.input.next(context)? {
                for (idx, acc) in accumulators.iter_mut().enumerate() {
                    acc.update_value(&self.aggr_exprs[idx].evaluate(&tuple)?)?;
                }
            } else {
                break;
            }
        }

        let values = accumulators
            .iter()
            .map(|acc| acc.evaluate())
            .collect::<BustubxResult<Vec<ScalarValue>>>()?;

        self.output_count.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Tuple::new(self.schema.clone(), values)))
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate")
    }
}
