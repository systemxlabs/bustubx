mod avg;
mod count;

pub use avg::AvgAccumulator;
pub use count::CountAccumulator;

use crate::catalog::{Column, DataType, Schema};
use crate::common::ScalarValue;
use crate::expression::{Expr, ExprTrait};
use crate::{BustubxError, BustubxResult, Tuple};
use std::fmt::Debug;
use strum::{EnumIter, IntoEnumIterator};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AggregateFunction {
    /// the function kind
    pub func_kind: AggregateFunctionKind,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
}

impl ExprTrait for AggregateFunction {
    fn data_type(&self, _input_schema: &Schema) -> BustubxResult<DataType> {
        match self.func_kind {
            AggregateFunctionKind::Count => Ok(DataType::Int64),
            AggregateFunctionKind::Avg => Ok(DataType::Float64),
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> BustubxResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, tuple: &Tuple) -> BustubxResult<ScalarValue> {
        match self.func_kind {
            AggregateFunctionKind::Count | AggregateFunctionKind::Avg => {
                let expr = self.args.first().ok_or(BustubxError::Internal(format!(
                    "aggregate function {} should have one arg instead of {:?}",
                    self.func_kind, self.args
                )))?;
                expr.evaluate(tuple)
            }
        }
    }

    fn to_column(&self, input_schema: &Schema) -> BustubxResult<Column> {
        Ok(Column::new(
            format!("{}", self),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func_kind)
    }
}

#[derive(Clone, PartialEq, Eq, Debug, EnumIter)]
pub enum AggregateFunctionKind {
    Count,
    Avg,
}

impl AggregateFunctionKind {
    pub fn create_accumulator(&self) -> Box<dyn Accumulator> {
        match self {
            AggregateFunctionKind::Count => Box::new(CountAccumulator::new()),
            AggregateFunctionKind::Avg => Box::new(AvgAccumulator::new()),
        }
    }

    pub fn find(name: &str) -> Option<Self> {
        AggregateFunctionKind::iter().find(|kind| kind.to_string().eq_ignore_ascii_case(name))
    }
}

impl std::fmt::Display for AggregateFunctionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub trait Accumulator: Send + Sync + Debug {
    /// Updates the accumulator's state from its input.
    fn update_value(&mut self, value: &ScalarValue) -> BustubxResult<()>;

    /// Returns the final aggregate value, consuming the internal state.
    fn evaluate(&self) -> BustubxResult<ScalarValue>;
}
