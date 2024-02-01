use crate::catalog::Schema;
use std::sync::Arc;

use crate::planner::logical_plan_v2::{
    CreateIndex, CreateTable, EmptyRelation, Filter, Insert, Join, Limit, LogicalPlanV2, Project,
    Sort, TableScan, Values,
};

use crate::planner::physical_plan::PhysicalCreateTable;
use crate::planner::physical_plan::PhysicalFilter;
use crate::planner::physical_plan::PhysicalInsert;
use crate::planner::physical_plan::PhysicalLimit;
use crate::planner::physical_plan::PhysicalNestedLoopJoin;
use crate::planner::physical_plan::PhysicalPlan;
use crate::planner::physical_plan::PhysicalProject;
use crate::planner::physical_plan::PhysicalSeqScan;
use crate::planner::physical_plan::PhysicalSort;
use crate::planner::physical_plan::PhysicalValues;
use crate::planner::physical_plan::{PhysicalCreateIndex, PhysicalEmpty};

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_physical_plan(&self, logical_plan: LogicalPlanV2) -> PhysicalPlan {
        let logical_plan = Arc::new(logical_plan);
        build_plan_v2(logical_plan)
    }
}

pub fn build_plan_v2(logical_plan: Arc<LogicalPlanV2>) -> PhysicalPlan {
    let plan = match logical_plan.as_ref() {
        LogicalPlanV2::CreateTable(CreateTable { name, columns }) => PhysicalPlan::CreateTable(
            PhysicalCreateTable::new(name.clone(), Schema::new(columns.clone())),
        ),
        LogicalPlanV2::CreateIndex(CreateIndex {
            index_name,
            table,
            table_schema,
            columns,
        }) => PhysicalPlan::CreateIndex(PhysicalCreateIndex::new(
            index_name.clone(),
            table.clone(),
            table_schema.clone(),
            columns.clone(),
        )),
        LogicalPlanV2::Insert(Insert {
            table,
            columns,
            input,
        }) => {
            let input_physical_plan = build_plan_v2(input.clone());
            PhysicalPlan::Insert(PhysicalInsert::new(
                table.clone(),
                columns.clone(),
                Arc::new(input_physical_plan),
            ))
        }
        LogicalPlanV2::Values(Values { schema, values }) => {
            PhysicalPlan::Values(PhysicalValues::new(schema.clone(), values.clone()))
        }
        LogicalPlanV2::Project(Project {
            exprs,
            input,
            schema,
        }) => {
            let input_physical_plan = build_plan_v2(input.clone());
            PhysicalPlan::Project(PhysicalProject::new(
                exprs.clone(),
                schema.clone(),
                Arc::new(input_physical_plan),
            ))
        }
        LogicalPlanV2::Filter(Filter { predicate, input }) => {
            let input_physical_plan = build_plan_v2(input.clone());
            PhysicalPlan::Filter(PhysicalFilter::new(
                predicate.clone(),
                Arc::new(input_physical_plan),
            ))
        }
        LogicalPlanV2::TableScan(TableScan {
            table_ref,
            table_schema,
            filters,
            limit,
        }) => PhysicalPlan::TableScan(PhysicalSeqScan::new(
            table_ref.clone(),
            table_schema.clone(),
        )),
        LogicalPlanV2::Limit(Limit {
            limit,
            offset,
            input,
        }) => {
            let input_physical_plan = build_plan_v2((*input).clone());
            PhysicalPlan::Limit(PhysicalLimit::new(
                limit.clone(),
                *offset,
                Arc::new(input_physical_plan),
            ))
        }
        LogicalPlanV2::Join(Join {
            left,
            right,
            join_type,
            condition,
            schema,
        }) => {
            let left_physical_plan = build_plan_v2((*left).clone());
            let right_physical_plan = build_plan_v2((*right).clone());
            PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin::new(
                join_type.clone(),
                condition.clone(),
                Arc::new(left_physical_plan),
                Arc::new(right_physical_plan),
                schema.clone(),
            ))
        }
        LogicalPlanV2::Sort(Sort {
            expr,
            ref input,
            limit,
        }) => {
            let input_physical_plan = build_plan_v2(Arc::clone(input));
            PhysicalPlan::Sort(PhysicalSort::new(
                expr.clone(),
                Arc::new(input_physical_plan),
            ))
        }
        LogicalPlanV2::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema,
        }) => PhysicalPlan::Empty(PhysicalEmpty {
            schema: schema.clone(),
        }),
    };
    plan
}
