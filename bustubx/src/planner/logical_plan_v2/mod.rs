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

use crate::catalog::{Column, DataType, Schema, SchemaRef};
pub use create_index::CreateIndex;
pub use create_table::CreateTable;
pub use empty_relation::EmptyRelation;
pub use filter::Filter;
pub use insert::Insert;
pub use join::Join;
pub use limit::Limit;
pub use project::Project;
pub use sort::{OrderByExpr, Sort};
use std::sync::Arc;
pub use table_scan::TableScan;
pub use util::*;
pub use values::Values;

#[derive(Debug, Clone)]
pub enum LogicalPlanV2 {
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

impl LogicalPlanV2 {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            LogicalPlanV2::CreateTable(_) => &Arc::new(Schema::empty()),
            LogicalPlanV2::CreateIndex(_) => &Arc::new(Schema::empty()),
            LogicalPlanV2::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlanV2::Insert(_) => &Arc::new(Schema::new(vec![Column::new(
                "insert_rows".to_string(),
                DataType::Int32,
            )])),
            LogicalPlanV2::Join(Join { schema, .. }) => schema,
            LogicalPlanV2::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlanV2::Project(Project { schema, .. }) => schema,
            LogicalPlanV2::TableScan(TableScan { schema, .. }) => schema,
            LogicalPlanV2::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlanV2::Values(Values { schema, .. }) => schema,
            LogicalPlanV2::EmptyRelation(EmptyRelation { schema, .. }) => schema,
        }
    }
}
