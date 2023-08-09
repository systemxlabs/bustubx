use crate::catalog::{catalog::TableOid, column::ColumnFullName, schema::Schema};

#[derive(Debug, Clone)]
pub struct BoundBaseTableRef {
    pub table: String,
    pub oid: TableOid,
    pub alias: Option<String>,
    pub schema: Schema,
}
impl BoundBaseTableRef {
    pub fn column_names(&self) -> Vec<ColumnFullName> {
        self.schema
            .columns
            .iter()
            .map(|column| ColumnFullName::new(Some(self.table.clone()), column.column_name.clone()))
            .collect()
    }
}
