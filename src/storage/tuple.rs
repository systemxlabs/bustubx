use crate::catalog::column::Column;
use crate::{
    catalog::{column::ColumnFullName, schema::Schema},
    common::{config::TransactionId, rid::Rid},
    dbtype::value::Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    pub insert_txn_id: TransactionId,
    pub delete_txn_id: TransactionId,
    pub is_deleted: bool,
}

#[derive(Debug, Clone)]
pub struct Tuple {
    pub rid: Rid,
    pub data: Vec<u8>,
}

impl Tuple {
    pub const INVALID_TUPLE: Self = Self {
        rid: Rid::INVALID_RID,
        data: vec![],
    };

    pub fn new(data: Vec<u8>) -> Self {
        Self {
            rid: Rid::INVALID_RID,
            data,
        }
    }

    pub fn new_with_rid(rid: Rid, data: Vec<u8>) -> Self {
        Self { rid, data }
    }

    pub fn empty(size: usize) -> Self {
        Self {
            rid: Rid::INVALID_RID,
            data: vec![0; size],
        }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        let mut data = vec![];
        for value in values {
            data.extend(value.to_bytes());
        }
        Self {
            rid: Rid::INVALID_RID,
            data,
        }
    }

    pub fn from_bytes(raw: &[u8]) -> Self {
        let data = raw.to_vec();
        Self {
            rid: Rid::INVALID_RID,
            data,
        }
    }

    // TODO add unit test to make sure this still works if tuple format changes
    pub fn from_tuples(tuples: Vec<(Tuple, Schema)>) -> Self {
        let mut data = vec![];
        for (tuple, schema) in tuples {
            data.extend(tuple.data);
        }
        Self {
            rid: Rid::INVALID_RID,
            data,
        }
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

    pub fn all_values(&self, schema: &Schema) -> Vec<Value> {
        let mut values = vec![];
        for column in &schema.columns {
            values.push(self.get_value_by_col(column));
        }
        values
    }

    pub fn get_value_by_col_id(&self, schema: &Schema, column_index: usize) -> Value {
        let column = schema
            .get_col_by_index(column_index)
            .expect("column not found");

        self.get_value_by_col(column)
    }
    pub fn get_value_by_col_name(&self, schema: &Schema, column_name: &ColumnFullName) -> Value {
        let column = schema
            .get_col_by_name(column_name)
            .expect("column not found");

        self.get_value_by_col(column)
    }

    pub fn get_value_by_col(&self, column: &Column) -> Value {
        let offset = column.column_offset;
        let len = column.fixed_len;
        // Intercept the byte sequence starting from offset,
        // and get length len from data as the current col row bytes.
        let raw = &self.data[offset..offset + len];

        Value::from_bytes(raw, column.column_type)
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

mod tests {
    use crate::storage::tuple::TupleMeta;
    use crate::{
        catalog::{column::Column, schema::Schema},
        dbtype::data_type::DataType,
    };
    use std::mem;

    #[test]
    pub fn test_compare() {
        let schema = Schema::new(vec![
            Column::new(None, "a".to_string(), DataType::TinyInt, 0),
            Column::new(None, "b".to_string(), DataType::SmallInt, 0),
        ]);
        let tuple1 = super::Tuple::new(vec![1u8, 1, 1]);
        let tuple2 = super::Tuple::new(vec![1u8, 1, 1]);
        let tuple3 = super::Tuple::new(vec![1u8, 2, 1]);
        let tuple4 = super::Tuple::new(vec![2u8, 1, 1]);
        let tuple5 = super::Tuple::new(vec![1u8, 0, 1]);

        assert_eq!(tuple1.compare(&tuple2, &schema), std::cmp::Ordering::Equal);
        assert_eq!(tuple1.compare(&tuple3, &schema), std::cmp::Ordering::Less);
        assert_eq!(tuple1.compare(&tuple4, &schema), std::cmp::Ordering::Less);
        assert_eq!(
            tuple1.compare(&tuple5, &schema),
            std::cmp::Ordering::Greater
        );
    }
}
