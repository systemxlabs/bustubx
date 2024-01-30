use std::sync::Arc;

use crate::planner::logical_plan::LogicalPlan;
use crate::planner::operator::LogicalOperator;

use crate::planner::physical_plan::PhysicalCreateIndex;
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

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_physical_plan(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        let logical_plan = Arc::new(logical_plan);
        build_plan(logical_plan.clone())
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
            PhysicalPlan::TableScan(PhysicalSeqScan::new(
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
