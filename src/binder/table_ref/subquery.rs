use crate::binder::statement::select::SelectStatement;

/// A subquery. e.g., `SELECT * FROM (SELECT * FROM t1)`, where `(SELECT * FROM t1)` is `BoundSubqueryRef`.
#[derive(Debug)]
pub struct BoundSubqueryRef {
    pub subquery: Box<SelectStatement>,
    pub select_list_name: Vec<String>,
    pub alias: String,
}
