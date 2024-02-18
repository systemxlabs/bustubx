use crate::catalog::SchemaRef;
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::Expr;
use crate::{BustubxResult, Tuple};
use std::sync::Arc;

pub struct PhysicalAggregate {
    /// The incoming physical plan
    pub input: Arc<PhysicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: SchemaRef,
}

impl VolcanoExecutor for PhysicalAggregate {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        todo!()
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
