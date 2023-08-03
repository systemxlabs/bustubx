use sqlparser::ast::{ColumnDef, ObjectName};

use crate::catalog::column::Column;

#[derive(Debug)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<Column>,
}
