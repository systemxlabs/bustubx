use crate::{
    catalog::{column::Column, schema::Schema},
    common::scalar::ScalarValue,
};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<ScalarValue>>,
}
