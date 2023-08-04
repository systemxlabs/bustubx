use crate::{
    binder::expression::BoundExpression,
    catalog::{
        catalog::TableOid,
        column::Column,
        schema::{self, Schema},
    },
    dbtype::value::Value,
};

use self::{
    create_table::LogicalCreateTableOperator, filter::LogicalFilterOperator,
    insert::LogicalInsertOperator, project::LogicalProjectOperator, scan::LogicalScanOperator,
    values::LogicalValuesOperator,
};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod project;
pub mod scan;
pub mod values;

#[derive(Debug)]
pub enum LogicalOperator {
    Dummy,
    CreateTable(LogicalCreateTableOperator),
    // Aggregate(AggregateOperator),
    Filter(LogicalFilterOperator),
    // Join(JoinOperator),
    Project(LogicalProjectOperator),
    Scan(LogicalScanOperator),
    // Sort(SortOperator),
    // Limit(LimitOperator),
    Insert(LogicalInsertOperator),
    Values(LogicalValuesOperator),
}
impl LogicalOperator {
    pub fn new_create_table_operator(table_name: String, schema: Schema) -> LogicalOperator {
        LogicalOperator::CreateTable(LogicalCreateTableOperator::new(table_name, schema))
    }
    pub fn new_insert_operator(table_name: String, columns: Vec<Column>) -> LogicalOperator {
        LogicalOperator::Insert(LogicalInsertOperator::new(table_name, columns))
    }
    pub fn new_values_operator(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> LogicalOperator {
        LogicalOperator::Values(LogicalValuesOperator::new(columns, tuples))
    }
    pub fn new_scan_operator(table_oid: TableOid, columns: Vec<Column>) -> LogicalOperator {
        LogicalOperator::Scan(LogicalScanOperator::new(table_oid, columns))
    }
    pub fn new_project_operator(expressions: Vec<BoundExpression>) -> LogicalOperator {
        LogicalOperator::Project(LogicalProjectOperator::new(expressions))
    }
    pub fn new_filter_operator(predicate: BoundExpression) -> LogicalOperator {
        LogicalOperator::Filter(LogicalFilterOperator::new(predicate))
    }
    pub fn output_schema(&self) -> Schema {
        match self {
            LogicalOperator::Dummy => Schema::new(vec![]),
            LogicalOperator::CreateTable(op) => op.output_schema(),
            LogicalOperator::Project(op) => op.output_schema(),
            LogicalOperator::Scan(op) => op.output_schema(),
            LogicalOperator::Insert(op) => op.output_schema(),
            LogicalOperator::Values(op) => op.output_schema(),
            LogicalOperator::Filter(op) => op.output_schema(),
        }
    }
}
