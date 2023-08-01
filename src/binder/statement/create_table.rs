use sqlparser::ast::{ColumnDef, ObjectName};

use crate::catalog::column::Column;

use super::BoundStatement;

#[derive(Debug)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<Column>,
}
impl CreateTableStatement {
    pub fn bind(name: &ObjectName, column_defs: &Vec<ColumnDef>) -> Self {
        let table_name = name.to_string();
        let columns = column_defs
            .iter()
            .map(|c| Column::from_sqlparser_column(c))
            .collect();
        CreateTableStatement {
            table_name,
            columns,
        }
    }
}
