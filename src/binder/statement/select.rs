use sqlparser::ast::{Query, SetExpr};

use crate::binder::{expression::BoundExpression, table_ref::BoundTableRef};

#[derive(Debug)]
pub struct SelectStatement {
    pub table: BoundTableRef,
    pub select_list: Vec<BoundExpression>,
    pub where_clause: BoundExpression,
}
