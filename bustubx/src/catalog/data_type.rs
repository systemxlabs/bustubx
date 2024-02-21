use crate::error::BustubxError;
use sqlparser::dialect::PostgreSqlDialect;

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
            sqlparser::ast::DataType::Integer(_) => Ok(DataType::Int32),
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

impl From<&DataType> for sqlparser::ast::DataType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Boolean => sqlparser::ast::DataType::Boolean,
            DataType::Int8 => sqlparser::ast::DataType::TinyInt(None),
            DataType::Int16 => sqlparser::ast::DataType::SmallInt(None),
            DataType::Int32 => sqlparser::ast::DataType::Integer(None),
            DataType::Int64 => sqlparser::ast::DataType::BigInt(None),
            DataType::UInt64 => sqlparser::ast::DataType::UnsignedBigInt(None),
            DataType::Float32 => sqlparser::ast::DataType::Float(None),
            DataType::Float64 => sqlparser::ast::DataType::Double,
            DataType::Varchar(len) => {
                sqlparser::ast::DataType::Varchar(len.map(|l| sqlparser::ast::CharacterLength {
                    length: l as u64,
                    unit: None,
                }))
            }
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => write!(f, "{self:?}"),
            DataType::Varchar(len_opt) => {
                if let Some(len) = len_opt {
                    write!(f, "Varchar({len})")
                } else {
                    write!(f, "Varchar")
                }
            }
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = BustubxError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut parser =
            sqlparser::parser::Parser::new(&PostgreSqlDialect {}).try_with_sql(value)?;
        let sql_data_type = parser.parse_data_type()?;
        (&sql_data_type).try_into()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::DataType;

    #[test]
    fn parse_data_type() {
        let sql_type: sqlparser::ast::DataType = (&DataType::Int32).into();
        assert_eq!(
            DataType::try_from(format!("{sql_type}").as_str()).unwrap(),
            DataType::Int32
        );

        let sql_type: sqlparser::ast::DataType = (&DataType::Varchar(Some(100))).into();
        assert_eq!(
            DataType::try_from(format!("{sql_type}").as_str()).unwrap(),
            DataType::Varchar(Some(100))
        );
    }
}
