use crate::catalog::schema::Schema;

use self::{
    create_table::PhysicalCreateTable, filter::PhysicalFilter, insert::PhysicalInsert,
    limit::PhysicalLimit, nested_loop_join::PhysicalNestedLoopJoin, project::PhysicalProject,
    table_scan::PhysicalTableScan, values::PhysicalValues,
};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod nested_loop_join;
pub mod project;
pub mod table_scan;
pub mod values;

#[derive(Debug)]
pub enum PhysicalOperator {
    Dummy,
    CreateTable(PhysicalCreateTable),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    TableScan(PhysicalTableScan),
    Limit(PhysicalLimit),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    NestedLoopJoin(PhysicalNestedLoopJoin),
}
impl PhysicalOperator {
    pub fn output_schema(&self) -> Schema {
        match self {
            Self::Dummy => Schema::new(vec![]),
            Self::CreateTable(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
            Self::Project(op) => op.output_schema(),
            Self::Filter(op) => op.output_schema(),
            Self::TableScan(op) => op.output_schema(),
            Self::Limit(op) => op.output_schema(),
            Self::NestedLoopJoin(op) => op.output_schema(),
        }
    }
}
