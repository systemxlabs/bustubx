use self::{base_table::BoundBaseTableRef, join::BoundJoinRef, subquery::BoundSubqueryRef};

pub mod base_table;
pub mod join;
pub mod subquery;

#[derive(Debug, Clone)]
pub enum BoundTableRef {
    BaseTable(BoundBaseTableRef),
    Join(BoundJoinRef),
    Subquery(BoundSubqueryRef),
}
