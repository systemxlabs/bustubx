use sqlparser::ast::{ObjectName, OrderByExpr};

use super::{statement::create_index::CreateIndexStatement, Binder};

impl<'a> Binder<'a> {
    pub fn bind_create_index(
        &self,
        index_name: &ObjectName,
        table_name: &ObjectName,
        columns: &Vec<OrderByExpr>,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: index_name.to_string(),
            table: self.bind_base_table_by_name(table_name.to_string().as_str(), None),
            columns: columns
                .iter()
                .map(|column| self.bind_column_ref_expr(&column.expr))
                .collect(),
        }
    }
}
