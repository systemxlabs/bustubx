use crate::{
    catalog::schema::Schema,
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
        self.data.iter().all(|&x| x == 0)
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
    pub fn get_value_by_col_id(&self, schema: &Schema, column_index: usize) -> Value {
        let column = schema.get_by_index(column_index).expect("column not found");
        let offset = column.column_offset;
        let len = column.fixed_len;
        let raw = &self.data[offset..offset + len];
        Value::from_bytes(raw, column.column_type)
    }
    pub fn get_value_by_col_name(&self, schema: &Schema, columon_name: &str) -> Value {
        let column = schema
            .get_by_col_name(columon_name)
            .expect("column not found");
        let offset = column.column_offset;
        let len = column.fixed_len;
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
    use crate::catalog::{
        column::{Column, DataType},
        schema::Schema,
    };

    #[test]
    pub fn test_compare() {
        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::TinyInt, 0),
            Column::new("b".to_string(), DataType::SmallInt, 0),
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
