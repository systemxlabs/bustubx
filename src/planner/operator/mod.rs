use crate::{
    catalog::{
        column::Column,
        schema::{self, Schema},
    },
    dbtype::value::Value,
};

use self::{
    create_table::LogicalCreateTableOperator, insert::LogicalInsertOperator,
    values::LogicalValuesOperator,
};

pub mod create_table;
pub mod insert;
pub mod values;

#[derive(Debug)]
pub enum LogicalOperator {
    Dummy,
    CreateTable(LogicalCreateTableOperator),
    // Aggregate(AggregateOperator),
    // Filter(FilterOperator),
    // Join(JoinOperator),
    // Project(ProjectOperator),
    // Scan(ScanOperator),
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
    pub fn output_schema(&self) -> Schema {
        match self {
            LogicalOperator::Dummy => Schema::new(vec![]),
            LogicalOperator::CreateTable(op) => op.output_schema(),
            LogicalOperator::Insert(op) => op.output_schema(),
            LogicalOperator::Values(op) => op.output_schema(),
        }
    }
}
