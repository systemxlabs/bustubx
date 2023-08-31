use crate::binder::{
    expression::column_ref::BoundColumnRef, table_ref::base_table::BoundBaseTableRef,
};

#[derive(Debug)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table: BoundBaseTableRef,
    pub columns: Vec<BoundColumnRef>,
}
