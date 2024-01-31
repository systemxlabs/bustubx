use crate::catalog::DataType;
use crate::catalog::Schema;
use crate::common::table_ref::TableReference;
use crate::common::ScalarValue;
use crate::error::{BustubxError, BustubxResult};
use crate::expression::ExprTrait;
use crate::storage::Tuple;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnExpr {
    /// relation/table reference.
    pub relation: Option<TableReference>,
    /// field/column name.
    pub name: String,
}

impl ExprTrait for ColumnExpr {
    fn data_type(&self, input_schema: &Schema) -> BustubxResult<DataType> {
        input_schema.get_col_by_name(&self.name).map_or(
            Err(BustubxError::Internal("Failed to get column".to_string())),
            |col| Ok(col.data_type),
        )
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        Ok(tuple.get_value_by_col_name(&tuple.schema, &self.name))
    }
}
