mod create_index;
mod create_table;
mod empty_relation;
mod filter;
mod insert;
mod join;
mod limit;
mod project;
mod sort;
mod table_scan;
mod util;
mod values;

use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF, INSERT_OUTPUT_SCHEMA_REF};
pub use create_index::CreateIndex;
pub use create_table::CreateTable;
pub use empty_relation::EmptyRelation;
pub use filter::Filter;
pub use insert::Insert;
pub use join::{Join, JoinType};
pub use limit::Limit;
pub use project::Project;
pub use sort::{OrderByExpr, Sort};
pub use table_scan::TableScan;
pub use util::*;
pub use values::Values;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Filter(Filter),
    Insert(Insert),
    Join(Join),
    Limit(Limit),
    Project(Project),
    TableScan(TableScan),
    Sort(Sort),
    Values(Values),
    EmptyRelation(EmptyRelation),
}

impl LogicalPlan {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            LogicalPlan::CreateTable(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::CreateIndex(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Insert(_) => &INSERT_OUTPUT_SCHEMA_REF,
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Project(Project { schema, .. }) => schema,
            LogicalPlan::TableScan(TableScan {
                table_schema: schema,
                ..
            }) => schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Values(Values { schema, .. }) => schema,
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema,
        }
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalPlan::CreateTable(v) => write!(f, "{v}"),
            LogicalPlan::CreateIndex(v) => write!(f, "{v}"),
            LogicalPlan::Filter(v) => write!(f, "{v}"),
            LogicalPlan::Insert(v) => write!(f, "{v}"),
            LogicalPlan::Join(v) => write!(f, "{v}"),
            LogicalPlan::Limit(v) => write!(f, "{v}"),
            LogicalPlan::Project(v) => write!(f, "{v}"),
            LogicalPlan::TableScan(v) => write!(f, "{v}"),
            LogicalPlan::Sort(v) => write!(f, "{v}"),
            LogicalPlan::Values(v) => write!(f, "{v}"),
            LogicalPlan::EmptyRelation(v) => write!(f, "{v}"),
        }
    }
}
