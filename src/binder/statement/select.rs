use sqlparser::ast::{Query, SetExpr};

use crate::binder::{expression::BoundExpression, table_ref::BoundTableRef};

#[derive(Debug)]
pub struct SelectStatement {
    pub table: BoundTableRef,
    pub select_list: Vec<BoundExpression>,
    pub where_clause: BoundExpression,
}
impl SelectStatement {
    pub fn bind(query: &Query) -> Self {
        match query.body.as_ref() {
            SetExpr::Select(ref select) => {
                // let table = BoundTableRef::bind(&select.from);
                // let select_list = select
                //     .projection
                //     .iter()
                //     .map(|e| BoundExpression::bind(e))
                //     .collect();
                // let where_clause = BoundExpression::bind(&select.selection);
                // SelectStatement {
                //     table,
                //     select_list,
                //     where_clause,
                // };
            }
            _ => unimplemented!(),
        }
        unimplemented!()
    }
}
