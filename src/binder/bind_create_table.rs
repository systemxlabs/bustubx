use sqlparser::ast::{ColumnDef, ObjectName};

use crate::catalog::column::Column;

use super::{statement::create_table::CreateTableStatement, Binder};

impl<'a> Binder<'a> {
    pub fn bind_create_table(
        &self,
        name: &ObjectName,
        column_defs: &Vec<ColumnDef>,
    ) -> CreateTableStatement {
        let table_name = name.to_string();
        let columns = column_defs
            .iter()
            .map(|c| Column::from_sqlparser_column(Some(table_name.clone()), c))
            .collect();
        CreateTableStatement {
            table_name,
            columns,
        }
    }
}
