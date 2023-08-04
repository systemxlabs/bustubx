use std::sync::Arc;

use crate::{binder::statement::select::SelectStatement, planner::operator::LogicalOperator};

use super::{logical_plan::LogicalPlan, Planner};

impl Planner {
    pub fn plan_select(&mut self, stmt: SelectStatement) -> LogicalPlan {
        let table_scan_plan = Arc::new(self.plan_table_ref(stmt.from_table));

        let filter_plan = stmt.where_clause.map(|predicate| LogicalPlan {
            operator: LogicalOperator::new_filter_operator(predicate),
            children: vec![table_scan_plan.clone()],
        });

        let project_plan = LogicalPlan {
            operator: LogicalOperator::new_project_operator(stmt.select_list),
            children: filter_plan
                .map_or(vec![table_scan_plan.clone()], |child| vec![Arc::new(child)]),
        };
        project_plan
    }
}
