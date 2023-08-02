use std::sync::Arc;

use crate::{catalog::column::Column, dbtype::value::Value};

use self::{insert::InsertOperator, values::ValuesOperator};

pub mod insert;
pub mod values;

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
    Insert(InsertOperator),
    Values(ValuesOperator),
}
impl LogicalOperator {
    pub fn new_insert_operator(table_name: String) -> LogicalOperator {
        LogicalOperator::Insert(InsertOperator::new(table_name))
    }
    pub fn new_values_operator(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> LogicalOperator {
        LogicalOperator::Values(ValuesOperator::new(columns, tuples))
    }
}
