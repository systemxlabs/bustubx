use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::common::TableReference;
use crate::expression::{ColumnExpr, Expr};
use crate::planner::logical_plan::OrderByExpr;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxError, BustubxResult,
};
use std::sync::Arc;

#[derive(Debug, derive_new::new)]
pub struct PhysicalCreateIndex {
    pub name: String,
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub columns: Vec<OrderByExpr>,
}

impl VolcanoExecutor for PhysicalCreateIndex {
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let mut key_indices = vec![];
        for col in self.columns.iter() {
            match col.expr.as_ref() {
                Expr::Column(ColumnExpr { name, .. }) => {
                    key_indices.push(self.table_schema.index_of(None, name)?);
                }
                _ => {
                    return Err(BustubxError::Execution(format!(
                        "The expr should be column instead of {}",
                        col.expr
                    )))
                }
            }
        }
        let key_schema = Arc::new(self.table_schema.project(&key_indices)?);
        context
            .catalog
            .create_index(self.name.clone(), &self.table, key_schema)?;
        Ok(None)
    }
    fn output_schema(&self) -> SchemaRef {
        EMPTY_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalCreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateIndex: {}", self.name)
    }
}
