use super::column::{Column, ColumnRef};
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnRef>,
}

impl Schema {
    pub fn new(mut columns: Vec<Column>) -> Self {
        let mut curr_offset = 0;
        let mut column_refs = Vec::new();
        for mut column in columns.into_iter() {
            // 计算每个column的offset
            column.column_offset = curr_offset;
            curr_offset += column.data_type.type_size();
            column_refs.push(Arc::new(column));
        }
        Self {
            columns: column_refs,
        }
    }

    pub fn from_schemas(schemas: Vec<Schema>) -> Self {
        let mut columns = Vec::new();
        for schema in schemas {
            columns.extend(schema.columns);
        }
        Self { columns }
    }

    pub fn copy_schema(from: &Schema, key_attrs: &[u32]) -> Self {
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
