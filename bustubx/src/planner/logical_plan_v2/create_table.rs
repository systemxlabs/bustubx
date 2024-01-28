use crate::catalog::Schema;
use crate::common::table_ref::TableReference;

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: TableReference,
    pub schema: Schema,
}
