use sqlparser::ast::Query;

use crate::binder::{expression::BoundExpression, table_ref::BoundTableRef};

#[derive(Debug)]
pub struct SelectStatement {
    pub table: BoundTableRef,
    pub select_list: Vec<BoundExpression>,
    pub where_clause: BoundExpression,
}
impl SelectStatement {
    pub fn bind(query: &Box<Query>) -> Self {
        unimplemented!()
    }
}
