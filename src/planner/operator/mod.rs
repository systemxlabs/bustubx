use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

use self::{insert::LogicalInsertOperator, values::LogicalValuesOperator};

pub mod insert;
pub mod values;

pub trait LogicalPlanNode {
    fn output_schema(&self) -> &Schema;
}

#[derive(Debug)]
pub enum LogicalOperator {
    Dummy,
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
    pub fn new_insert_operator(table_name: String, columns: Vec<Column>) -> LogicalOperator {
        LogicalOperator::Insert(LogicalInsertOperator::new(table_name, columns))
    }
    pub fn new_values_operator(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> LogicalOperator {
        LogicalOperator::Values(LogicalValuesOperator::new(columns, tuples))
    }
    pub fn output_schema(&self) -> Schema {
        match self {
            LogicalOperator::Dummy => Schema::new(vec![]),
            // LogicalOperator::Aggregate(op) => op.output_schema(),
            // LogicalOperator::Filter(op) => op.output_schema(),
            // LogicalOperator::Join(op) => op.output_schema(),
            // LogicalOperator::Project(op) => op.output_schema(),
            // LogicalOperator::Scan(op) => op.output_schema(),
            // LogicalOperator::Sort(op) => op.output_schema(),
            // LogicalOperator::Limit(op) => op.output_schema(),
            LogicalOperator::Insert(op) => op.output_schema(),
            LogicalOperator::Values(op) => op.output_schema(),
        }
    }
}
