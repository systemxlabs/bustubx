use crate::catalog::{ColumnRef, SchemaRef};
use crate::common::{TableReference, TransactionId};
use crate::{catalog::Schema, common::ScalarValue, BustubxError, BustubxResult};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    pub insert_txn_id: TransactionId,
    pub delete_txn_id: TransactionId,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Tuple {
    pub schema: SchemaRef,
    pub data: Vec<ScalarValue>,
}

impl Tuple {
    pub fn new(schema: SchemaRef, data: Vec<ScalarValue>) -> Self {
        Self { schema, data }
    }

    pub fn empty(schema: SchemaRef) -> Self {
        let mut data = vec![];
        for col in schema.columns.iter() {
            data.push(ScalarValue::new_empty(col.data_type));
        }
        Self { schema, data }
    }

    pub fn try_merge(tuples: impl IntoIterator<Item = Self>) -> BustubxResult<Self> {
        let mut data = vec![];
        let mut merged_schema = Schema::empty();
        for tuple in tuples {
            data.extend(tuple.data);
            merged_schema = Schema::try_merge(vec![merged_schema, tuple.schema.as_ref().clone()])?;
        }
        Ok(Self {
            schema: Arc::new(merged_schema),
            data,
        })
    }

    pub fn is_null(&self) -> bool {
        self.data.iter().all(|x| x.is_null())
    }

    pub fn value(&self, index: usize) -> BustubxResult<&ScalarValue> {
        self.data.get(index).ok_or(BustubxError::Internal(format!(
            "Not found column data at {} in tuple: {:?}",
            index, self
        )))
    }
    pub fn value_by_name(
        &self,
        relation: Option<&TableReference>,
        name: &str,
    ) -> BustubxResult<&ScalarValue> {
        let idx = self.schema.index_of(relation, name)?;
        self.value(idx)
    }

    // TODO 比较索引key大小
    pub fn compare(&self, other: &Self, schema: &Schema) -> std::cmp::Ordering {
        let column_count = schema.column_count();
        for column_index in 0..column_count {
            let compare_res = self
                .value(column_index)
                .unwrap()
                .partial_cmp(&other.value(column_index).unwrap())
                .unwrap();
            if compare_res == std::cmp::Ordering::Equal {
                continue;
            }
            if compare_res == std::cmp::Ordering::Less {
                return std::cmp::Ordering::Less;
            }
            if compare_res == std::cmp::Ordering::Greater {
                return std::cmp::Ordering::Greater;
            }
        }
        return std::cmp::Ordering::Equal;
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use std::sync::Arc;

    #[test]
    pub fn test_compare() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a".to_string(), DataType::Int8, false),
            Column::new("b".to_string(), DataType::Int16, false),
        ]));
        let tuple1 = super::Tuple::new(schema.clone(), vec![1i8.into(), 2i16.into()]);
        let tuple2 = super::Tuple::new(schema.clone(), vec![1i8.into(), 2i16.into()]);
        let tuple3 = super::Tuple::new(schema.clone(), vec![1i8.into(), 3i16.into()]);
        let tuple4 = super::Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]);
        let tuple5 = super::Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]);

        assert_eq!(tuple1.compare(&tuple2, &schema), std::cmp::Ordering::Equal);
        assert_eq!(tuple1.compare(&tuple3, &schema), std::cmp::Ordering::Less);
        assert_eq!(tuple1.compare(&tuple4, &schema), std::cmp::Ordering::Less);
        assert_eq!(
            tuple1.compare(&tuple5, &schema),
            std::cmp::Ordering::Greater
        );
    }
}
