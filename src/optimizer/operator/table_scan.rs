use crate::catalog::{catalog::TableOid, column::Column, schema::Schema};

#[derive(Debug)]
pub struct PhysicalTableScan {
    pub table_oid: TableOid,
    pub columns: Vec<Column>,
}
impl PhysicalTableScan {
    pub fn new(table_oid: TableOid, columns: Vec<Column>) -> Self {
        PhysicalTableScan { table_oid, columns }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(self.columns.clone())
    }
}
