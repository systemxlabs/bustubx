use crate::catalog::Schema;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalCreateTableOperator {
    pub table_name: String,
    pub schema: Schema,
}
