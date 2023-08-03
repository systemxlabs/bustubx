use std::sync::Arc;

use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

use super::operator::{
    insert::PhysicalInsertOperator, values::PhysicalValuesOperator, PhysicalOperator,
};

#[derive(Debug)]
pub struct PhysicalPlan {
    pub operator: PhysicalOperator,
    pub children: Vec<Arc<PhysicalPlan>>,
}
impl PhysicalPlan {
    pub fn output_schema(&self) -> Schema {
        self.operator.output_schema()
    }
    pub fn dummy() -> Self {
        Self {
            operator: PhysicalOperator::Dummy,
            children: Vec::new(),
        }
    }
    pub fn new_insert_node(table_name: &String, columns: &Vec<Column>) -> Self {
        Self {
            operator: PhysicalOperator::Insert(PhysicalInsertOperator::new(
                table_name.clone(),
                columns.clone(),
            )),
            children: Vec::new(),
        }
    }
    pub fn new_values_node(columns: &Vec<Column>, tuples: &Vec<Vec<Value>>) -> Self {
        Self {
            operator: PhysicalOperator::Values(PhysicalValuesOperator::new(
                columns.clone(),
                tuples.clone(),
            )),
            children: Vec::new(),
        }
    }
}
