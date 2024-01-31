use super::column::{Column, ColumnRef};
use crate::error::BustubxResult;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

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

    pub fn get_index_by_name(&self, col_name: &String) -> Option<usize> {
        self.columns.iter().position(|c| &c.name == col_name)
    }

    pub fn fixed_len(&self) -> usize {
        self.columns.iter().map(|c| c.data_type.type_size()).sum()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
