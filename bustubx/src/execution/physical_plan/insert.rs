use std::sync::{atomic::AtomicU32, Arc};
use tracing::debug;

use crate::catalog::{SchemaRef, INSERT_OUTPUT_SCHEMA_REF};
use crate::common::TableReference;
use crate::{
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::{Tuple, TupleMeta},
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    pub input: Arc<PhysicalPlan>,

    insert_rows: AtomicU32,
}
impl PhysicalInsert {
    pub fn new(
        table: TableReference,
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            table,
            table_schema,
            projected_schema,
            input,
            insert_rows: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalInsert {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init insert executor");
        self.input.init(context)?;
        self.insert_rows
            .store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        loop {
            let next_tuple = self.input.next(context)?;
            if next_tuple.is_none() {
                // only return insert_rows when input exhausted
                return if self.insert_rows.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    let insert_rows = self.insert_rows.load(std::sync::atomic::Ordering::SeqCst);
                    self.insert_rows
                        .store(0, std::sync::atomic::Ordering::SeqCst);
                    Ok(Some(Tuple::new(
                        self.output_schema(),
                        vec![ScalarValue::Int32(Some(insert_rows as i32))],
                    )))
                };
            }
            let tuple = next_tuple.unwrap();

            // cast values
            let mut casted_data = vec![];
            for (idx, value) in tuple.data.iter().enumerate() {
                casted_data
                    .push(value.cast_to(&self.projected_schema.column_with_index(idx)?.data_type)?);
            }
            let tuple = Tuple {
                schema: self.projected_schema.clone(),
                data: casted_data,
            };

            // TODO update index if needed
            let table_heap = &mut context
                .catalog
                .get_mut_table_by_name(self.table.table())
                .unwrap()
                .table;
            let tuple_meta = TupleMeta {
                insert_txn_id: 0,
                delete_txn_id: 0,
                is_deleted: false,
            };
            table_heap.insert_tuple(&tuple_meta, &tuple)?;
            self.insert_rows
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn output_schema(&self) -> SchemaRef {
        INSERT_OUTPUT_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert")
    }
}
