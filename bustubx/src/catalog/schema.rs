use super::column::{Column, ColumnRef};
use crate::catalog::DataType;
use crate::error::BustubxResult;
use crate::BustubxError;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

lazy_static::lazy_static! {
    pub static ref EMPTY_SCHEMA_REF: SchemaRef = Arc::new(Schema::empty());
    pub static ref INSERT_OUTPUT_SCHEMA_REF: SchemaRef = Arc::new(Schema::new(
        vec![Column::new("insert_rows".to_string(), DataType::Int32)]
    ));
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnRef>,
}

impl Schema {
    pub fn new(mut columns: Vec<Column>) -> Self {
        Self {
            columns: columns.into_iter().map(|col| Arc::new(col)).collect(),
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
            .map(|i| {
                self.get_col_by_index(*i).ok_or_else(|| {
                    BustubxError::Plan(format!(
                        "project index {} out of bounds, max column count {}",
                        i,
                        self.columns.len(),
                    ))
                })
            })
            .collect::<BustubxResult<Vec<ColumnRef>>>()?;
        Ok(Arc::new(Schema {
            columns: new_columns,
        }))
    }

    pub fn copy_schema(from: SchemaRef, key_attrs: &[u32]) -> Self {
        let columns = key_attrs
            .iter()
            .map(|i| from.columns[*i as usize].clone())
            .collect();
        Schema { columns }
    }

    pub fn get_col_by_name(&self, col_name: &String) -> Option<ColumnRef> {
        self.columns.iter().find(|c| &c.name == col_name).cloned()
    }

    pub fn get_col_by_index(&self, index: usize) -> Option<ColumnRef> {
        self.columns.get(index).cloned()
    }

    pub fn column_with_index(&self, index: usize) -> BustubxResult<ColumnRef> {
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| BustubxError::Plan(format!("Unable to get column with index {index}")))
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> BustubxResult<usize> {
        let (idx, _) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| &col.name == name)
            .ok_or_else(|| BustubxError::Plan(format!("Unable to get column named \"{name}\"")))?;
        Ok(idx)
    }

    pub fn fixed_len(&self) -> usize {
        self.columns.iter().map(|c| c.data_type.type_size()).sum()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
