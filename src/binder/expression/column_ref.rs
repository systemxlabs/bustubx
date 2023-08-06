use crate::{catalog::schema::Schema, dbtype::value::Value, storage::tuple::Tuple};

/// A bound column reference, e.g., `y.x` in the SELECT list.
#[derive(Debug, Clone)]
pub struct BoundColumnRef {
    pub col_names: Vec<String>,
}
impl BoundColumnRef {
    pub fn evaluate(&self, tuple: Option<&Tuple>, schema: Option<&Schema>) -> Value {
        if tuple.is_none() || schema.is_none() {
            panic!("tuple or schema is none")
        }
        let tuple = tuple.unwrap();
        let schema = schema.unwrap();
        tuple.get_value_by_col_name(schema, self.col_names[0].as_str())
    }
}
