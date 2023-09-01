use crate::{binder::statement::create_table::CreateTableStatement, catalog::schema::Schema};

use super::{logical_plan::LogicalPlan, operator::LogicalOperator, Planner};

impl Planner {
    pub fn plan_create_table(&self, stmt: CreateTableStatement) -> LogicalPlan {
        let schema = Schema::new(stmt.columns);
        LogicalPlan {
            operator: LogicalOperator::new_create_table_operator(stmt.table_name, schema),
            children: Vec::new(),
        }
    }
}
