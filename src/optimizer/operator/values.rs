use crate::{catalog::column::Column, dbtype::value::Value};

#[derive(Debug)]
pub struct PhysicalValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,
}
impl PhysicalValuesOperator {
    pub fn new(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> Self {
        PhysicalValuesOperator { columns, tuples }
    }
}
