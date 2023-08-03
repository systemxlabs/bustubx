use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

#[derive(Debug)]
pub struct PhysicalValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,
}
impl PhysicalValuesOperator {
    pub fn new(columns: Vec<Column>, tuples: Vec<Vec<Value>>) -> Self {
        PhysicalValuesOperator { columns, tuples }
    }
    pub fn output_schema(&self) -> Schema {
        return Schema::new(self.columns.clone());
    }
}
