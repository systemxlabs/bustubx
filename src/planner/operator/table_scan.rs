use crate::catalog::{catalog::TableOid, column::Column, schema::Schema};

#[derive(Debug)]
pub struct LogicalTableScanOperator {
    pub table_oid: TableOid,
    pub columns: Vec<Column>,
}
impl LogicalTableScanOperator {
    pub fn new(table_oid: TableOid, column: Vec<Column>) -> Self {
        Self {
            table_oid,
            columns: column,
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(self.columns.clone())
    }
}
