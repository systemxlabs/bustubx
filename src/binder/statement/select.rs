use crate::binder::{expression::BoundExpression, table_ref::BoundTableRef};

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_list: Vec<BoundExpression>,
    pub from_table: BoundTableRef,
    pub where_clause: Option<BoundExpression>,
    pub limit: Option<BoundExpression>,
    pub offset: Option<BoundExpression>,
}
