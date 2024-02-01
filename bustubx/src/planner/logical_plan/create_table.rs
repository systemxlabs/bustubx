use crate::catalog::Column;
use crate::common::table_ref::TableReference;

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: TableReference,
    pub columns: Vec<Column>,
}
