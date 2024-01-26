use std::sync::{atomic::AtomicU32, Arc};

use crate::{
    catalog::{column::Column, data_type::DataType, schema::Schema},
    common::scalar::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::{Tuple, TupleMeta},
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub input: Arc<PhysicalPlan>,

    insert_rows: AtomicU32,
}
impl PhysicalInsert {
    pub fn new(table_name: String, columns: Vec<Column>, input: Arc<PhysicalPlan>) -> Self {
        Self {
            table_name,
            columns,
            input,
            insert_rows: AtomicU32::new(0),
        }
    }
    pub fn output_schema(&self) -> Schema {
        Schema::new(vec![Column::new(
            "insert_rows".to_string(),
            DataType::Int32,
            0,
        )])
    }
}
impl VolcanoExecutor for PhysicalInsert {
    fn init(&self, context: &mut ExecutionContext) {
        println!("init insert executor");
        self.insert_rows
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.input.init(context);
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        loop {
            let next_tuple = self.input.next(context);
            if next_tuple.is_none() {
                // only return insert_rows when input exhausted
                if self.insert_rows.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    return None;
                } else {
                    let insert_rows = self.insert_rows.load(std::sync::atomic::Ordering::SeqCst);
                    self.insert_rows
                        .store(0, std::sync::atomic::Ordering::SeqCst);
                    return Some(Tuple::from_values(vec![ScalarValue::Int32(Some(
                        insert_rows as i32,
                    ))]));
                }
            }

            let tuple = next_tuple.unwrap();
            // TODO update index if needed
            let table_heap = &mut context
                .catalog
                .get_mut_table_by_name(self.table_name.as_str())
                .unwrap()
                .table;
            let tuple_meta = TupleMeta {
                insert_txn_id: 0,
                delete_txn_id: 0,
                is_deleted: false,
            };
            // TODO check result
            table_heap.insert_tuple(&tuple_meta, &tuple);
            self.insert_rows
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}
