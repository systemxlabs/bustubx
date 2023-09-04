use crate::{
    catalog::{column::Column, schema::Schema},
    dbtype::value::Value,
};

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalValuesOperator {
    pub columns: Vec<Column>,
    pub tuples: Vec<Vec<Value>>,
}
