use self::{insert::PhysicalInsertOperator, values::PhysicalValuesOperator};

pub mod insert;
pub mod values;

#[derive(Debug)]
pub enum PhysicalOperator {
    Dummy,
    Insert(PhysicalInsertOperator),
    Values(PhysicalValuesOperator),
}
