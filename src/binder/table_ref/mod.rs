use crate::catalog::column::ColumnFullName;

use self::{base_table::BoundBaseTableRef, join::BoundJoinRef, subquery::BoundSubqueryRef};

use super::expression::{column_ref::BoundColumnRef, BoundExpression};

pub mod base_table;
pub mod join;
pub mod subquery;

#[derive(Debug, Clone)]
pub enum BoundTableRef {
    BaseTable(BoundBaseTableRef),
    Join(BoundJoinRef),
    Subquery(BoundSubqueryRef),
}
impl BoundTableRef {
    pub fn column_names(&self) -> Vec<ColumnFullName> {
        match self {
            BoundTableRef::BaseTable(table_ref) => table_ref.column_names(),
            BoundTableRef::Join(join_ref) => join_ref.column_names(),
            BoundTableRef::Subquery(subquery_ref) => subquery_ref.column_names(),
        }
    }
    pub fn gen_select_list(&self) -> Vec<BoundExpression> {
        self.column_names()
            .iter()
            .map(|c| {
                BoundExpression::ColumnRef(BoundColumnRef {
                    col_name: c.clone(),
                })
            })
            .collect::<Vec<BoundExpression>>()
    }
}
