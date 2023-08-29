use std::sync::Arc;

use crate::{
    catalog::schema::Schema,
    execution::{ExecutionContext, VolcanoExecutorV2},
    planner::{logical_plan::LogicalPlan, operator::LogicalOperator},
    storage::tuple::Tuple,
};

use self::{
    create_table::PhysicalCreateTable, filter::PhysicalFilter, insert::PhysicalInsert,
    limit::PhysicalLimit, nested_loop_join::PhysicalNestedLoopJoin, project::PhysicalProject,
    table_scan::PhysicalTableScan, values::PhysicalValues,
};

pub mod create_table;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod nested_loop_join;
pub mod project;
pub mod table_scan;
pub mod values;

#[derive(Debug)]
pub enum PhysicalPlanV2 {
    Dummy,
    CreateTable(PhysicalCreateTable),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    TableScan(PhysicalTableScan),
    Limit(PhysicalLimit),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    NestedLoopJoin(PhysicalNestedLoopJoin),
}
impl PhysicalPlanV2 {
    pub fn output_schema(&self) -> Schema {
        match self {
            Self::Dummy => Schema::new(vec![]),
            Self::CreateTable(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
            Self::Project(op) => op.output_schema(),
            Self::Filter(op) => op.output_schema(),
            Self::TableScan(op) => op.output_schema(),
            Self::Limit(op) => op.output_schema(),
            Self::NestedLoopJoin(op) => op.output_schema(),
        }
    }
}

pub fn build_plan_v2(logical_plan: Arc<LogicalPlan>) -> PhysicalPlanV2 {
    let plan = match logical_plan.operator {
        LogicalOperator::Dummy => PhysicalPlanV2::Dummy,
        LogicalOperator::CreateTable(ref logic_create_table) => {
            PhysicalPlanV2::CreateTable(PhysicalCreateTable::new(
                logic_create_table.table_name.clone(),
                logic_create_table.schema.clone(),
            ))
        }
        LogicalOperator::Insert(ref logic_insert) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan_v2(child_logical_node.clone());
            PhysicalPlanV2::Insert(PhysicalInsert::new(
                logic_insert.table_name.clone(),
                logic_insert.columns.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Values(ref logical_values) => PhysicalPlanV2::Values(PhysicalValues::new(
            logical_values.columns.clone(),
            logical_values.tuples.clone(),
        )),
        LogicalOperator::Project(ref logical_project) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan_v2(child_logical_node.clone());
            PhysicalPlanV2::Project(PhysicalProject::new(
                logical_project.expressions.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Filter(ref logical_filter) => {
            // filter下只有一个子节点
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan_v2(child_logical_node.clone());
            PhysicalPlanV2::Filter(PhysicalFilter::new(
                logical_filter.predicate.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Scan(ref logical_table_scan) => {
            PhysicalPlanV2::TableScan(PhysicalTableScan::new(
                logical_table_scan.table_oid.clone(),
                logical_table_scan.columns.clone(),
            ))
        }
        LogicalOperator::Limit(ref logical_limit) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan_v2(child_logical_node.clone());
            PhysicalPlanV2::Limit(PhysicalLimit::new(
                logical_limit.limit,
                logical_limit.offset,
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Join(ref logical_nested_loop_join) => {
            let left_logical_node = logical_plan.children[0].clone();
            let left_physical_node = build_plan_v2(left_logical_node.clone());
            let right_logical_node = logical_plan.children[1].clone();
            let right_physical_node = build_plan_v2(right_logical_node.clone());
            PhysicalPlanV2::NestedLoopJoin(PhysicalNestedLoopJoin::new(
                logical_nested_loop_join.join_type.clone(),
                logical_nested_loop_join.condition.clone(),
                Arc::new(left_physical_node),
                Arc::new(right_physical_node),
            ))
        }
        _ => unimplemented!(),
    };
    plan
}
impl VolcanoExecutorV2 for PhysicalPlanV2 {
    fn init(&self, context: &mut ExecutionContext) {
        match self {
            PhysicalPlanV2::Dummy => {}
            PhysicalPlanV2::CreateTable(op) => op.init(context),
            PhysicalPlanV2::Insert(op) => op.init(context),
            PhysicalPlanV2::Values(op) => op.init(context),
            PhysicalPlanV2::Project(op) => op.init(context),
            PhysicalPlanV2::Filter(op) => op.init(context),
            PhysicalPlanV2::TableScan(op) => op.init(context),
            PhysicalPlanV2::Limit(op) => op.init(context),
            PhysicalPlanV2::NestedLoopJoin(op) => op.init(context),
        }
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        match self {
            PhysicalPlanV2::Dummy => None,
            PhysicalPlanV2::CreateTable(op) => op.next(context),
            PhysicalPlanV2::Insert(op) => op.next(context),
            PhysicalPlanV2::Values(op) => op.next(context),
            PhysicalPlanV2::Project(op) => op.next(context),
            PhysicalPlanV2::Filter(op) => op.next(context),
            PhysicalPlanV2::TableScan(op) => op.next(context),
            PhysicalPlanV2::Limit(op) => op.next(context),
            PhysicalPlanV2::NestedLoopJoin(op) => op.next(context),
        }
    }
}
