use crate::catalog::column::ColumnRef;
use crate::common::scalar::ScalarValue;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalValuesOperator {
    pub columns: Vec<ColumnRef>,
    pub tuples: Vec<Vec<ScalarValue>>,
}
