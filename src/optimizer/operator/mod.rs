use crate::catalog::schema::Schema;

use self::{insert::PhysicalInsertOperator, values::PhysicalValuesOperator};

pub mod insert;
pub mod values;

#[derive(Debug)]
pub enum PhysicalOperator {
    Dummy,
    Insert(PhysicalInsertOperator),
    Values(PhysicalValuesOperator),
}
impl PhysicalOperator {
    pub fn output_schema(&self) -> Schema {
        match self {
            Self::Dummy => Schema::new(vec![]),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
        }
    }
}
