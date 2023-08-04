use std::sync::Arc;

use crate::{
    binder::{statement::BoundStatement, table_ref::BoundTableRef},
    catalog::schema::{self, Schema},
};

use self::{logical_plan::LogicalPlan, operator::LogicalOperator};

pub mod logical_plan;
pub mod operator;
pub mod plan_select;

pub struct Planner {}
impl Planner {
    // 根据BoundStatement生成逻辑计划
    pub fn plan(&mut self, statement: BoundStatement) -> LogicalPlan {
        match statement {
            BoundStatement::Insert(stmt) => {
                let values_node = LogicalPlan {
                    operator: LogicalOperator::new_values_operator(
                        stmt.columns.clone(),
                        stmt.values,
                    ),
                    children: Vec::new(),
                };
                LogicalPlan {
                    operator: LogicalOperator::new_insert_operator(stmt.table.table, stmt.columns),
                    children: vec![Arc::new(values_node)],
                }
            }
            BoundStatement::CreateTable(stmt) => {
                let schema = Schema::new(stmt.columns);
                LogicalPlan {
                    operator: LogicalOperator::new_create_table_operator(stmt.table_name, schema),
                    children: Vec::new(),
                }
            }
            BoundStatement::Select(stmt) => self.plan_select(stmt),
            _ => unimplemented!(),
        }
    }

    fn plan_table_ref(&mut self, table_ref: BoundTableRef) -> LogicalPlan {
        match table_ref {
            BoundTableRef::BaseTable(table) => LogicalPlan {
                operator: LogicalOperator::new_table_scan_operator(table.oid, table.schema.columns),
                children: Vec::new(),
            },
            _ => unimplemented!(),
        }
    }
}
