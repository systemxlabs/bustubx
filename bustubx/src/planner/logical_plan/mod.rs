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

use crate::catalog::SchemaRef;
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
            LogicalPlan::CreateTable(_) => todo!(),
            LogicalPlan::CreateIndex(_) => todo!(),
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Insert(_) => todo!(),
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
