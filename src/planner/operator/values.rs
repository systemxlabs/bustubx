use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

#[derive(Debug)]
pub struct LogicalValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,
}
impl LogicalValuesOperator {
    pub fn new(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> Self {
        Self { columns, tuples }
    }
}
