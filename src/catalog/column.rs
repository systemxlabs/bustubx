use sqlparser::ast::ColumnDef;

use crate::catalog::data_type::DataType;

// 列定义
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    // 内联列则为固定列的大小，否则为指针大小
    pub fixed_len: usize,
    // 列在元组中的偏移量
    pub column_offset: usize,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            fixed_len: data_type.type_size(),
            column_offset: 0,
        }
    }

    pub fn from_sqlparser_column(column_def: &ColumnDef) -> Self {
        let column_name = column_def.name.to_string();
        let column_type: DataType = (&column_def.data_type).try_into().unwrap();
        Self::new(column_name, column_type)
    }
}
