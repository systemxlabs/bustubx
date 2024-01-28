use crate::catalog::column::ColumnRef;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalInsertOperator {
    pub table_name: String,
    pub columns: Vec<ColumnRef>,
}
