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
