use std::sync::Arc;

use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalLimitOperator {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub input: Arc<PhysicalOperator>,
}
impl PhysicalLimitOperator {
    pub fn new(limit: Option<usize>, offset: Option<usize>, input: Arc<PhysicalOperator>) -> Self {
        PhysicalLimitOperator {
            limit,
            offset,
            input,
        }
    }
    pub fn output_schema(&self) -> Schema {
        return self.input.output_schema();
    }
}
