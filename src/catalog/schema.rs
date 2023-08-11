use super::column::{Column, ColumnFullName};

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}
impl Schema {
    pub fn new(mut columns: Vec<Column>) -> Self {
        let mut curr_offset = 0;
        for column in columns.iter_mut() {
            // 计算每个column的offset
            column.column_offset = curr_offset;
            curr_offset += column.fixed_len;
        }
        Self { columns }
    }

    pub fn copy_schema(from: &Schema, key_attrs: &[u32]) -> Self {
        let columns = key_attrs
            .iter()
            .map(|i| from.columns[*i as usize].clone())
            .collect();
        Self::new(columns)
    }

    pub fn get_col_by_name(&self, col_full_name: &ColumnFullName) -> Option<&Column> {
        // if table name not specified, then match column with the column name
        self.columns.iter().find(|c| {
            if col_full_name.table.is_none() {
                c.full_name.column == col_full_name.column
            } else {
                c.full_name == *col_full_name
            }
        })
    }

    pub fn get_col_by_index(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    pub fn is_inlined(&self) -> bool {
        self.columns.iter().all(|c| c.is_inlined())
    }

    pub fn fixed_len(&self) -> usize {
        self.columns.iter().map(|c| c.fixed_len).sum()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
