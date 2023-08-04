use crate::catalog::{catalog::TableOid, column::Column, schema::Schema};

#[derive(Debug)]
pub struct PhysicalTableScanOperator {
    pub table_oid: TableOid,
    pub columns: Vec<Column>,
}
impl PhysicalTableScanOperator {
    pub fn new(table_oid: TableOid, columns: Vec<Column>) -> Self {
        PhysicalTableScanOperator { table_oid, columns }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(self.columns.clone())
    }
}
