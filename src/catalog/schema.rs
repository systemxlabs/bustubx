use super::column::Column;

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
            curr_offset += column.data_type.type_size();
        }
        Self { columns }
    }

    pub fn from_schemas(schemas: Vec<Schema>) -> Self {
        let mut columns = Vec::new();
        for schema in schemas {
            columns.extend(schema.columns);
        }
        Self::new(columns)
    }

    pub fn copy_schema(from: &Schema, key_attrs: &[u32]) -> Self {
        let columns = key_attrs
            .iter()
            .map(|i| from.columns[*i as usize].clone())
            .collect();
        Self::new(columns)
    }

    pub fn get_col_by_name(&self, col_name: &String) -> Option<&Column> {
        self.columns.iter().find(|c| &c.name == col_name)
    }

    pub fn get_col_by_index(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
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
