use crate::catalog::schema::Schema;

use self::{
    create_table::PhysicalCreateTableOperator, insert::PhysicalInsertOperator,
    values::PhysicalValuesOperator,
};

pub mod create_table;
pub mod insert;
pub mod values;

#[derive(Debug)]
pub enum PhysicalOperator {
    Dummy,
    CreateTable(PhysicalCreateTableOperator),
    Insert(PhysicalInsertOperator),
    Values(PhysicalValuesOperator),
}
impl PhysicalOperator {
    pub fn output_schema(&self) -> Schema {
        match self {
            Self::Dummy => Schema::new(vec![]),
            Self::CreateTable(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
        }
    }
}
