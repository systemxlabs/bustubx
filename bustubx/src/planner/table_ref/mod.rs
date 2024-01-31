use self::{base_table::BoundBaseTableRef, join::BoundJoinRef};
use crate::expression::{ColumnExpr, Expr};

pub mod base_table;
pub mod join;

#[derive(Debug, Clone)]
pub enum BoundTableRef {
    BaseTable(BoundBaseTableRef),
    Join(BoundJoinRef),
}
impl BoundTableRef {
    pub fn column_names(&self) -> Vec<String> {
        match self {
            BoundTableRef::BaseTable(table_ref) => table_ref.column_names(),
            BoundTableRef::Join(join_ref) => join_ref.column_names(),
        }
    }
    pub fn gen_select_list(&self) -> Vec<Expr> {
        self.column_names()
            .iter()
            .map(|c| {
                Expr::Column(ColumnExpr {
                    relation: None,
                    name: c.clone(),
                })
            })
            .collect::<Vec<Expr>>()
    }
}
