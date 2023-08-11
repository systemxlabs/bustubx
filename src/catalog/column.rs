use sqlparser::ast::ColumnDef;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Varchar,
    Timestamp,
}
impl DataType {
    pub fn type_size(&self) -> usize {
        match self {
            DataType::Boolean => 1,
            DataType::TinyInt => 1,
            DataType::SmallInt => 2,
            DataType::Integer => 4,
            DataType::BigInt => 8,
            DataType::Decimal => 8,
            // TODO 指针大小，暂时跟bustub保持一致
            DataType::Varchar => 12,
            DataType::Timestamp => 8,
        }
    }

    pub fn from_sqlparser_data_type(data_type: &sqlparser::ast::DataType) -> Self {
        match data_type {
            sqlparser::ast::DataType::Boolean => DataType::Boolean,
            sqlparser::ast::DataType::TinyInt(_) => DataType::TinyInt,
            sqlparser::ast::DataType::SmallInt(_) => DataType::SmallInt,
            sqlparser::ast::DataType::Int(_) => DataType::Integer,
            sqlparser::ast::DataType::BigInt(_) => DataType::BigInt,
            sqlparser::ast::DataType::Decimal { .. } => DataType::Decimal,
            sqlparser::ast::DataType::Char(_) => DataType::Varchar,
            sqlparser::ast::DataType::Varchar(_) => DataType::Varchar,
            sqlparser::ast::DataType::Timestamp(_, _) => DataType::Timestamp,
            _ => unimplemented!(),
        }
    }
}
