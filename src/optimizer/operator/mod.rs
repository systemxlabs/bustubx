use crate::catalog::schema::Schema;

use self::{
    create_table::PhysicalCreateTableOperator, filter::PhysicalFilterOperator,
    insert::PhysicalInsertOperator, limit::PhysicalLimitOperator, project::PhysicalProjectOperator,
    table_scan::PhysicalTableScanOperator, values::PhysicalValuesOperator,
};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod project;
pub mod table_scan;
pub mod values;

#[derive(Debug)]
pub enum PhysicalOperator {
    Dummy,
    CreateTable(PhysicalCreateTableOperator),
    Project(PhysicalProjectOperator),
    Filter(PhysicalFilterOperator),
    TableScan(PhysicalTableScanOperator),
    Limit(PhysicalLimitOperator),
    Insert(PhysicalInsertOperator),
    Values(PhysicalValuesOperator),
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
        }
    }
    pub fn is_insert(&self) -> bool {
        match self {
            Self::Insert(_) => true,
            _ => false,
        }
    }
}
