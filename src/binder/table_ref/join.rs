use crate::{binder::expression::BoundExpression, catalog::column::ColumnFullName};

use super::BoundTableRef;

#[derive(Debug, Clone)]
pub enum JoinType {
    // select * from x inner join y on ...
    Inner,
    // select * from x left (outer) join y on ...
    LeftOuter,
    // select * from x right (outer) join y on ...
    RightOuter,
    // select * from x full (outer) join y on ...
    FullOuter,
    // select * from x, y
    // select * from x cross join y
    CrossJoin,
}

/// A join. e.g., `SELECT * FROM x INNER JOIN y ON ...`, where `x INNER JOIN y ON ...` is `BoundJoinRef`.
#[derive(Debug, Clone)]
pub struct BoundJoinRef {
    pub join_type: JoinType,
    pub left: Box<BoundTableRef>,
    pub right: Box<BoundTableRef>,
    pub condition: Option<BoundExpression>,
}
impl BoundJoinRef {
    pub fn column_names(&self) -> Vec<ColumnFullName> {
        let mut columns = self.left.column_names();
        columns.extend(self.right.column_names());
        columns
    }
}
