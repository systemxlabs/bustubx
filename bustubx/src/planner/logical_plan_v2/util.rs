use crate::catalog::{ColumnRef, Schema};
use crate::planner::table_ref::join::JoinType;
use crate::BustubxResult;
use std::sync::Arc;

pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: JoinType,
) -> BustubxResult<Schema> {
    fn nullify_columns(columns: &[ColumnRef]) -> Vec<ColumnRef> {
        columns
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect()
    }

    let left_cols = &left.columns;
    let right_cols = &right.columns;

    let columns: Vec<ColumnRef> = match join_type {
        JoinType::Inner | JoinType::CrossJoin => {
            left_cols.iter().chain(right_cols.iter()).cloned().collect()
        }
        JoinType::LeftOuter => left_cols
            .iter()
            .chain(&nullify_columns(right_cols))
            .cloned()
            .collect(),
        JoinType::RightOuter => nullify_columns(left_cols)
            .iter()
            .chain(right_cols.iter())
            .cloned()
            .collect(),
        JoinType::FullOuter => nullify_columns(left_cols)
            .iter()
            .chain(&nullify_columns(right_cols))
            .cloned()
            .collect(),
    };
    Ok(Schema { columns })
}
