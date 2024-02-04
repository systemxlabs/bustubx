mod create_index;
mod create_table;
mod empty;
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
pub use empty::PhysicalEmpty;
pub use filter::PhysicalFilter;
pub use insert::PhysicalInsert;
pub use limit::PhysicalLimit;
pub use nested_loop_join::PhysicalNestedLoopJoin;
pub use project::PhysicalProject;
pub use seq_scan::PhysicalSeqScan;
pub use sort::PhysicalSort;
pub use values::PhysicalValues;

use crate::catalog::SchemaRef;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(PhysicalEmpty),
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

impl PhysicalPlan {
    pub fn inputs(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::Project(PhysicalProject { input, .. }) => vec![input],
            PhysicalPlan::Filter(PhysicalFilter { input, .. }) => vec![input],
            PhysicalPlan::Limit(PhysicalLimit { input, .. }) => vec![input],
            PhysicalPlan::Insert(PhysicalInsert { input, .. }) => vec![input],
            PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin {
                left_input,
                right_input,
                ..
            }) => vec![left_input, right_input],
            PhysicalPlan::Sort(PhysicalSort { input, .. }) => vec![input],
            PhysicalPlan::Empty(_)
            | PhysicalPlan::CreateTable(_)
            | PhysicalPlan::CreateIndex(_)
            | PhysicalPlan::TableScan(_)
            | PhysicalPlan::Values(_) => vec![],
        }
    }
}

impl VolcanoExecutor for PhysicalPlan {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        match self {
            PhysicalPlan::Empty(op) => op.init(context),
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

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        match self {
            PhysicalPlan::Empty(op) => op.next(context),
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
            Self::Empty(op) => op.output_schema(),
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

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty(op) => write!(f, "{op}"),
            Self::CreateTable(op) => write!(f, "{op}"),
            Self::CreateIndex(op) => write!(f, "{op}"),
            Self::Insert(op) => write!(f, "{op}"),
            Self::Values(op) => write!(f, "{op}"),
            Self::Project(op) => write!(f, "{op}"),
            Self::Filter(op) => write!(f, "{op}"),
            Self::TableScan(op) => write!(f, "{op}"),
            Self::Limit(op) => write!(f, "{op}"),
            Self::NestedLoopJoin(op) => write!(f, "{op}"),
            Self::Sort(op) => write!(f, "{op}"),
        }
    }
}
