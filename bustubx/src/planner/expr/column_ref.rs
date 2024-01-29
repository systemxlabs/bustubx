use crate::common::table_ref::TableReference;
use crate::{catalog::Schema, common::ScalarValue, storage::Tuple};

/// A bound column reference, e.g., `y.x` in the SELECT list.
#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub relation: Option<TableReference>,
    pub col_name: String,
}
impl ColumnRef {
    pub fn evaluate(&self, tuple: Option<&Tuple>, schema: Option<&Schema>) -> ScalarValue {
        if tuple.is_none() || schema.is_none() {
            panic!("tuple or schema is none")
        }
        let tuple = tuple.unwrap();
        let schema = schema.unwrap();
        tuple.get_value_by_col_name(schema, &self.col_name)
    }
}
