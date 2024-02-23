use super::column::{Column, ColumnRef};
use crate::catalog::DataType;
use crate::common::TableReference;
use crate::error::BustubxResult;
use crate::BustubxError;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

lazy_static::lazy_static! {
    pub static ref EMPTY_SCHEMA_REF: SchemaRef = Arc::new(Schema::empty());
    pub static ref INSERT_OUTPUT_SCHEMA_REF: SchemaRef = Arc::new(Schema::new(
        vec![Column::new("insert_rows", DataType::Int32, false)]
    ));
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    pub columns: Vec<ColumnRef>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns: columns.into_iter().map(Arc::new).collect(),
        }
    }

    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn try_merge(schemas: impl IntoIterator<Item = Self>) -> BustubxResult<Self> {
        // TODO check column conflict
        let mut columns = Vec::new();
        for schema in schemas {
            columns.extend(schema.columns);
        }
        Ok(Self { columns })
    }

    pub fn project(&self, indices: &[usize]) -> BustubxResult<SchemaRef> {
        let new_columns = indices
            .iter()
            .map(|i| self.column_with_index(*i))
            .collect::<BustubxResult<Vec<ColumnRef>>>()?;
        Ok(Arc::new(Schema {
            columns: new_columns,
        }))
    }

    pub fn column_with_name(
        &self,
        relation: Option<&TableReference>,
        name: &str,
    ) -> BustubxResult<ColumnRef> {
        let index = self.index_of(relation, name)?;
        Ok(self.columns[index].clone())
    }

    pub fn column_with_index(&self, index: usize) -> BustubxResult<ColumnRef> {
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| BustubxError::Plan(format!("Unable to get column with index {index}")))
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, relation: Option<&TableReference>, name: &str) -> BustubxResult<usize> {
        let (idx, _) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| match (relation, &col.relation) {
                (Some(rel), Some(col_rel)) => rel.resolved_eq(col_rel) && name == &col.name,
                (Some(_), None) => false,
                (None, Some(_)) | (None, None) => name == &col.name,
            })
            .ok_or_else(|| BustubxError::Plan(format!("Unable to get column named \"{name}\"")))?;
        Ok(idx)
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
