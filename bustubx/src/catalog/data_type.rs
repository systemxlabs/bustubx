use crate::error::BustubxError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Varchar(Option<usize>),
}

impl TryFrom<&sqlparser::ast::DataType> for DataType {
    type Error = BustubxError;

    fn try_from(value: &sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Boolean => Ok(DataType::Boolean),
            sqlparser::ast::DataType::TinyInt(_) => Ok(DataType::Int8),
            sqlparser::ast::DataType::SmallInt(_) => Ok(DataType::Int16),
            sqlparser::ast::DataType::Int(_) => Ok(DataType::Int32),
            sqlparser::ast::DataType::BigInt(_) => Ok(DataType::Int64),
            sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            sqlparser::ast::DataType::Float(_) => Ok(DataType::Float32),
            sqlparser::ast::DataType::Double => Ok(DataType::Float32),
            sqlparser::ast::DataType::Varchar(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            sqlparser::ast::DataType::CharVarying(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            sqlparser::ast::DataType::CharacterVarying(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            _ => Err(BustubxError::NotSupport(format!(
                "Not support datatype {}",
                value
            ))),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
