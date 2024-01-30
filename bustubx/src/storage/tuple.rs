use crate::catalog::{ColumnRef, SchemaRef};
use crate::{catalog::Schema, common::config::TransactionId, common::ScalarValue, BustubxResult};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    pub insert_txn_id: TransactionId,
    pub delete_txn_id: TransactionId,
    pub is_deleted: bool,
}

#[derive(Debug, Clone)]
pub struct Tuple {
    pub schema: SchemaRef,
    pub data: Vec<u8>,
}

impl Tuple {
    pub fn new(schema: SchemaRef, data: Vec<u8>) -> Self {
        Self { schema, data }
    }

    pub fn empty(schema: SchemaRef, size: usize) -> Self {
        Self {
            schema,
            data: vec![0; size],
        }
    }

    pub fn from_values(schema: SchemaRef, values: Vec<ScalarValue>) -> Self {
        let mut data = vec![];
        for value in values {
            data.extend(value.to_bytes());
        }
        Self { schema, data }
    }

    pub fn from_bytes(schema: SchemaRef, raw: &[u8]) -> Self {
        let data = raw.to_vec();
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

    pub fn is_zero(&self) -> bool {
        // Iterate over each element in the 'data' vector using the 'iter' method.
        // The closure '|&x| x == 0' checks if each element is equal to 0.
        // The 'all' method returns 'true' if the closure returns 'true' for all elements.
        self.data.iter().all(|&x| x == 0)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub fn all_values(&self, schema: &Schema) -> Vec<ScalarValue> {
        let mut values = vec![];
        for column in &schema.columns {
            values.push(self.get_value_by_col(column.clone()));
        }
        values
    }

    pub fn get_value_by_col_id(&self, schema: &Schema, column_index: usize) -> ScalarValue {
        let column = schema
            .get_col_by_index(column_index)
            .expect("column not found");

        self.get_value_by_col(column)
    }
    pub fn get_value_by_col_name(&self, schema: &Schema, column_name: &String) -> ScalarValue {
        let column = schema
            .get_col_by_name(column_name)
            .expect("column not found");

        self.get_value_by_col(column)
    }

    pub fn get_value_by_col(&self, column: ColumnRef) -> ScalarValue {
        let offset = column.column_offset;
        let len = column.data_type.type_size();
        // Intercept the byte sequence starting from offset,
        // and get length len from data as the current col row bytes.
        let raw = &self.data[offset..offset + len];

        ScalarValue::from_bytes(raw, column.data_type)
    }

    // TODO 比较索引key大小
    pub fn compare(&self, other: &Self, schema: &Schema) -> std::cmp::Ordering {
        let column_count = schema.column_count();
        for column_index in 0..column_count {
            let compare_res = self
                .get_value_by_col_id(schema, column_index)
                .compare(&other.get_value_by_col_id(schema, column_index));
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
            Column::new("a".to_string(), DataType::Int8),
            Column::new("b".to_string(), DataType::Int16),
        ]));
        let tuple1 = super::Tuple::new(schema.clone(), vec![1u8, 1, 1]);
        let tuple2 = super::Tuple::new(schema.clone(), vec![1u8, 1, 1]);
        let tuple3 = super::Tuple::new(schema.clone(), vec![1u8, 2, 1]);
        let tuple4 = super::Tuple::new(schema.clone(), vec![2u8, 1, 1]);
        let tuple5 = super::Tuple::new(schema.clone(), vec![1u8, 0, 1]);

        assert_eq!(tuple1.compare(&tuple2, &schema), std::cmp::Ordering::Equal);
        assert_eq!(tuple1.compare(&tuple3, &schema), std::cmp::Ordering::Less);
        assert_eq!(tuple1.compare(&tuple4, &schema), std::cmp::Ordering::Less);
        assert_eq!(
            tuple1.compare(&tuple5, &schema),
            std::cmp::Ordering::Greater
        );
    }
}
