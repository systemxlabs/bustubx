use crate::common::table_ref::TableReference;
use crate::{catalog::Schema, common::ScalarValue, storage::Tuple};

/// A bound column reference, e.g., `y.x` in the SELECT list.
#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub relation: Option<TableReference>,
    pub col_name: String,
}
impl ColumnRef {
    pub fn evaluate(&self, tuple: Option<&Tuple>) -> ScalarValue {
        if tuple.is_none() {
            panic!("tuple or schema is none")
        }
        let tuple = tuple.unwrap();
        tuple.get_value_by_col_name(&tuple.schema, &self.col_name)
    }
}
