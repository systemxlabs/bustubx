use crate::{catalog::column::Column, dbtype::value::Value};

#[derive(Debug)]
pub struct ValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,
}
impl ValuesOperator {
    pub fn new(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> Self {
        ValuesOperator { columns, tuples }
    }
}
