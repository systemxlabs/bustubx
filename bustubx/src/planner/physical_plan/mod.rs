use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
};

mod create_index;
mod create_table;
mod filter;
mod insert;
mod limit;
mod nested_loop_join;
mod project;
mod seq_scan;
mod sort;
mod values;

pub use create_index::PhysicalCreateIndex;
pub use create_table::PhysicalCreateTable;
pub use filter::PhysicalFilter;
pub use insert::PhysicalInsert;
pub use limit::PhysicalLimit;
pub use nested_loop_join::PhysicalNestedLoopJoin;
pub use project::PhysicalProject;
pub use seq_scan::PhysicalSeqScan;
pub use sort::PhysicalSort;
pub use values::PhysicalValues;

#[derive(Debug)]
pub enum PhysicalPlan {
    Dummy,
    CreateTable(PhysicalCreateTable),
    CreateIndex(PhysicalCreateIndex),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    TableScan(PhysicalSeqScan),
    Limit(PhysicalLimit),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    Sort(PhysicalSort),
}

impl VolcanoExecutor for PhysicalPlan {
    fn init(&self, context: &mut ExecutionContext) {
        match self {
            PhysicalPlan::Dummy => {}
            PhysicalPlan::CreateTable(op) => op.init(context),
            PhysicalPlan::CreateIndex(op) => op.init(context),
            PhysicalPlan::Insert(op) => op.init(context),
            PhysicalPlan::Values(op) => op.init(context),
            PhysicalPlan::Project(op) => op.init(context),
            PhysicalPlan::Filter(op) => op.init(context),
            PhysicalPlan::TableScan(op) => op.init(context),
            PhysicalPlan::Limit(op) => op.init(context),
            PhysicalPlan::NestedLoopJoin(op) => op.init(context),
            PhysicalPlan::Sort(op) => op.init(context),
        }
    }

    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        match self {
            PhysicalPlan::Dummy => None,
            PhysicalPlan::CreateTable(op) => op.next(context),
            PhysicalPlan::CreateIndex(op) => op.next(context),
            PhysicalPlan::Insert(op) => op.next(context),
            PhysicalPlan::Values(op) => op.next(context),
            PhysicalPlan::Project(op) => op.next(context),
            PhysicalPlan::Filter(op) => op.next(context),
            PhysicalPlan::TableScan(op) => op.next(context),
            PhysicalPlan::Limit(op) => op.next(context),
            PhysicalPlan::NestedLoopJoin(op) => op.next(context),
            PhysicalPlan::Sort(op) => op.next(context),
        }
    }

    fn output_schema(&self) -> SchemaRef {
        match self {
            Self::Dummy => Arc::new(Schema::new(vec![])),
            Self::CreateTable(op) => op.output_schema(),
            Self::CreateIndex(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
            Self::Project(op) => op.output_schema(),
            Self::Filter(op) => op.output_schema(),
            Self::TableScan(op) => op.output_schema(),
            Self::Limit(op) => op.output_schema(),
            Self::NestedLoopJoin(op) => op.output_schema(),
            Self::Sort(op) => op.output_schema(),
        }
    }
}
