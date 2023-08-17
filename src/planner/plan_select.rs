use std::sync::Arc;

use crate::{
    binder::{
        expression::{constant::Constant, BoundExpression},
        statement::select::SelectStatement,
    },
    planner::operator::LogicalOperator,
};

use super::{logical_plan::LogicalPlan, Planner};

impl Planner {
    pub fn plan_select(&mut self, stmt: SelectStatement) -> LogicalPlan {
        // from table
        let mut plan = self.plan_table_ref(stmt.from_table);

        // filter
        if stmt.where_clause.is_some() {
            let mut filter_plan = LogicalPlan {
                operator: LogicalOperator::new_filter_operator(stmt.where_clause.unwrap()),
                children: Vec::new(),
            };
            filter_plan.children.push(Arc::new(plan));
            plan = filter_plan;
        }

        // project
        let mut plan = LogicalPlan {
            operator: LogicalOperator::new_project_operator(stmt.select_list),
            children: vec![Arc::new(plan)],
        };

        // TODO sort should be here
        // order by clause may use computed column, so it should be after project
        // for example, `select a+b from t order by a+b limit 10`

        // limit
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let mut limit_plan = self.plan_limit(&stmt.limit, &stmt.offset);
            limit_plan.children.push(Arc::new(plan));
            plan = limit_plan;
        }

        plan
    }

    pub fn plan_limit(
        &self,
        limit: &Option<BoundExpression>,
        offset: &Option<BoundExpression>,
    ) -> LogicalPlan {
        let limit = limit.as_ref().map(|limit| match limit {
            BoundExpression::Constant(ref constant) => match constant.value {
                Constant::Number(ref v) => v.parse::<usize>().unwrap(),
                _ => panic!("limit must be a number"),
            },
            _ => panic!("limit must be a number"),
        });
        let offset = offset.as_ref().map(|offset| match offset {
            BoundExpression::Constant(ref constant) => match constant.value {
                Constant::Number(ref v) => v.parse::<usize>().unwrap(),
                _ => panic!("offset must be a number"),
            },
            _ => panic!("offset must be a number"),
        });
        LogicalPlan {
            operator: LogicalOperator::new_limit_operator(limit, offset),
            children: Vec::new(),
        }
    }
}
