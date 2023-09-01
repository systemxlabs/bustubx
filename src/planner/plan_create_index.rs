use crate::binder::statement::create_index::CreateIndexStatement;

use super::{logical_plan::LogicalPlan, operator::LogicalOperator, Planner};

impl Planner {
    pub fn plan_create_index(&self, stmt: CreateIndexStatement) -> LogicalPlan {
        let table_schema = stmt.table.schema;
        let mut key_attrs = Vec::new();
        for col in stmt.columns {
            let index = table_schema
                .get_index_by_name(&col.col_name)
                .expect("col not found");
            key_attrs.push(index as u32);
        }
        LogicalPlan {
            operator: LogicalOperator::new_create_index_operator(
                stmt.index_name,
                stmt.table.table,
                table_schema,
                key_attrs,
            ),
            children: Vec::new(),
        }
    }
}
