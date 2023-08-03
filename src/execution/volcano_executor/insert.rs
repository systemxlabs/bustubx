use crate::catalog::column::Column;

pub struct InsertExecutor {
    pub table_name: String,
    pub columns: Vec<Column>,
}
