use crate::{binder::statement::select::SelectStatement, catalog::column::ColumnFullName};

/// A subquery. e.g., `SELECT * FROM (SELECT * FROM t1)`, where `(SELECT * FROM t1)` is `BoundSubqueryRef`.
#[derive(Debug, Clone)]
pub struct BoundSubqueryRef {
    pub subquery: Box<SelectStatement>,
    pub select_list_name: Vec<String>,
    pub alias: String,
}
impl BoundSubqueryRef {
    pub fn column_names(&self) -> Vec<ColumnFullName> {
        self.select_list_name
            .iter()
            .map(|name| ColumnFullName::new(Some(self.alias.clone()), name.clone()))
            .collect()
    }
}
