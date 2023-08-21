use crate::binder::{
    expression::BoundExpression, order_by::BoundOrderBy, table_ref::BoundTableRef,
};

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_list: Vec<BoundExpression>,
    pub from_table: BoundTableRef,
    pub where_clause: Option<BoundExpression>,
    pub limit: Option<BoundExpression>,
    pub offset: Option<BoundExpression>,
    pub sort: Vec<BoundOrderBy>,
}
