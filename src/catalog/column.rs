use sqlparser::ast::ColumnDef;

use crate::dbtype::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFullName {
    // table name or table alias
    pub table: Option<String>,
    // column name or column alias
    pub column: String,
}
impl ColumnFullName {
    pub fn new(table: Option<String>, column: String) -> Self {
        Self { table, column }
    }
}

// 列定义
#[derive(Debug, Clone)]
pub struct Column {
    pub full_name: ColumnFullName,
    pub column_type: DataType,
    // 内联列则为固定列的大小，否则为指针大小
    pub fixed_len: usize,
    // 内联列则为0，否则为变长列的大小
    pub variable_len: usize,
    // 列在元组中的偏移量
    pub column_offset: usize,
}

impl Column {
    pub fn new(
        table_name: Option<String>,
        column_name: String,
        column_type: DataType,
        variable_len: usize,
    ) -> Self {
        Self {
            full_name: ColumnFullName::new(table_name, column_name),
            column_type,
            fixed_len: column_type.type_size(),
            variable_len,
            column_offset: 0,
        }
    }

    pub fn from_sqlparser_column(table_name: Option<String>, column_def: &ColumnDef) -> Self {
        let column_name = column_def.name.to_string();
        let column_type = DataType::from_sqlparser_data_type(&column_def.data_type);
        Self::new(table_name, column_name, column_type, 0)
    }

    pub fn is_inlined(&self) -> bool {
        self.column_type != DataType::Varchar
    }
}
