use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;
use crate::{
    catalog::Schema,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

use self::{
    create_index::PhysicalCreateIndex, create_table::PhysicalCreateTable, filter::PhysicalFilter,
    insert::PhysicalInsert, limit::PhysicalLimit, nested_loop_join::PhysicalNestedLoopJoin,
    project::PhysicalProject, sort::PhysicalSort, table_scan::PhysicalTableScan,
    values::PhysicalValues,
};

pub mod create_index;
pub mod create_table;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod nested_loop_join;
pub mod project;
pub mod sort;
pub mod table_scan;
pub mod values;

#[derive(Debug)]
pub enum PhysicalPlan {
    Dummy,
    CreateTable(PhysicalCreateTable),
    CreateIndex(PhysicalCreateIndex),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    TableScan(PhysicalTableScan),
    Limit(PhysicalLimit),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    Sort(PhysicalSort),
}
impl PhysicalPlan {
    pub fn output_schema(&self) -> Schema {
        match self {
            Self::Dummy => Schema::new(vec![]),
            Self::CreateTable(op) => op.output_schema(),
            Self::CreateIndex(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
            Self::Project(op) => op.output_schema(),
            Self::Filter(op) => op.output_schema(),
            Self::TableScan(op) => op.output_schema(),
            Self::Limit(op) => op.output_schema(),
            Self::NestedLoopJoin(op) => op.output_schema(),
            Self::Sort(op) => op.output_schema(),
        }
    }
}

pub fn build_plan(logical_plan: Arc<LogicalPlan>) -> PhysicalPlan {
    let plan = match logical_plan.operator {
        LogicalOperator::Dummy => PhysicalPlan::Dummy,
        LogicalOperator::CreateTable(ref logic_create_table) => {
            PhysicalPlan::CreateTable(PhysicalCreateTable::new(
                logic_create_table.table_name.clone(),
                logic_create_table.schema.clone(),
            ))
        }
        LogicalOperator::CreateIndex(ref logic_create_index) => {
            PhysicalPlan::CreateIndex(PhysicalCreateIndex::new(
                logic_create_index.index_name.clone(),
                logic_create_index.table_name.clone(),
                logic_create_index.table_schema.clone(),
                logic_create_index.key_attrs.clone(),
            ))
        }
        LogicalOperator::Insert(ref logic_insert) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan(child_logical_node.clone());
            PhysicalPlan::Insert(PhysicalInsert::new(
                logic_insert.table_name.clone(),
                logic_insert.columns.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Values(ref logical_values) => PhysicalPlan::Values(PhysicalValues::new(
            logical_values.columns.clone(),
            logical_values.tuples.clone(),
        )),
        LogicalOperator::Project(ref logical_project) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan(child_logical_node.clone());
            PhysicalPlan::Project(PhysicalProject::new(
                logical_project.expressions.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Filter(ref logical_filter) => {
            // filter下只有一个子节点
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan(child_logical_node.clone());
            PhysicalPlan::Filter(PhysicalFilter::new(
                logical_filter.predicate.clone(),
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Scan(ref logical_table_scan) => {
            PhysicalPlan::TableScan(PhysicalTableScan::new(
                logical_table_scan.table_oid.clone(),
                logical_table_scan.columns.clone(),
            ))
        }
        LogicalOperator::Limit(ref logical_limit) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan(child_logical_node.clone());
            PhysicalPlan::Limit(PhysicalLimit::new(
                logical_limit.limit,
                logical_limit.offset,
                Arc::new(child_physical_node),
            ))
        }
        LogicalOperator::Join(ref logical_join) => {
            let left_logical_node = logical_plan.children[0].clone();
            let left_physical_node = build_plan(left_logical_node.clone());
            let right_logical_node = logical_plan.children[1].clone();
            let right_physical_node = build_plan(right_logical_node.clone());
            PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin::new(
                logical_join.join_type.clone(),
                logical_join.condition.clone(),
                Arc::new(left_physical_node),
                Arc::new(right_physical_node),
            ))
        }
        LogicalOperator::Sort(ref logical_sort) => {
            let child_logical_node = logical_plan.children[0].clone();
            let child_physical_node = build_plan(child_logical_node.clone());
            PhysicalPlan::Sort(PhysicalSort::new(
                logical_sort.order_bys.clone(),
                Arc::new(child_physical_node),
            ))
        }
        _ => unimplemented!(),
    };
    plan
}

impl VolcanoExecutor for PhysicalPlan {
    fn init(&self, context: &mut ExecutionContext) {
        match self {
            PhysicalPlan::Dummy => {}
            PhysicalPlan::CreateTable(op) => op.init(context),
            PhysicalPlan::CreateIndex(op) => op.init(context),
            PhysicalPlan::Insert(op) => op.init(context),
            PhysicalPlan::Values(op) => op.init(context),
            PhysicalPlan::Project(op) => op.init(context),
            PhysicalPlan::Filter(op) => op.init(context),
            PhysicalPlan::TableScan(op) => op.init(context),
            PhysicalPlan::Limit(op) => op.init(context),
            PhysicalPlan::NestedLoopJoin(op) => op.init(context),
            PhysicalPlan::Sort(op) => op.init(context),
        }
    }
    fn next(&self, context: &mut ExecutionContext) -> Option<Tuple> {
        match self {
            PhysicalPlan::Dummy => None,
            PhysicalPlan::CreateTable(op) => op.next(context),
            PhysicalPlan::CreateIndex(op) => op.next(context),
            PhysicalPlan::Insert(op) => op.next(context),
            PhysicalPlan::Values(op) => op.next(context),
            PhysicalPlan::Project(op) => op.next(context),
            PhysicalPlan::Filter(op) => op.next(context),
            PhysicalPlan::TableScan(op) => op.next(context),
            PhysicalPlan::Limit(op) => op.next(context),
            PhysicalPlan::NestedLoopJoin(op) => op.next(context),
            PhysicalPlan::Sort(op) => op.next(context),
        }
    }
}
