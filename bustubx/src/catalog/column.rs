use derive_with::With;
use sqlparser::ast::ColumnDef;
use std::sync::Arc;

use crate::catalog::DataType;

pub type ColumnRef = Arc<Column>;

#[derive(Debug, Clone, With)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.data_type == other.data_type
    }
}

impl Column {
    // TODO set nullable
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            nullable: false,
        }
    }
}
