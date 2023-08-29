use std::sync::Arc;

use crate::{
    binder::{expression::BoundExpression, table_ref::join::JoinType},
    catalog::{
        catalog::TableOid,
        column::Column,
        schema::{self, Schema},
    },
    dbtype::value::Value,
};

use super::operator::{
    create_table::PhysicalCreateTable, filter::PhysicalFilter, insert::PhysicalInsert,
    limit::PhysicalLimit, nested_loop_join::PhysicalNestedLoopJoin, project::PhysicalProject,
    table_scan::PhysicalTableScan, values::PhysicalValues, PhysicalPlanV2,
};

#[derive(Debug)]
pub struct PhysicalPlan {
    pub operator: Arc<PhysicalPlanV2>,
    pub children: Vec<Arc<PhysicalPlan>>,
}
impl PhysicalPlan {
    pub fn output_schema(&self) -> Schema {
        self.operator.output_schema()
    }
    pub fn dummy() -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Dummy),
            children: Vec::new(),
        }
    }
    pub fn new_create_table_node(table_name: &String, schema: &Schema) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::CreateTable(PhysicalCreateTable::new(
                table_name.clone(),
                schema.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_insert_node(
        table_name: &String,
        columns: &Vec<Column>,
        input: Arc<PhysicalPlanV2>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Insert(PhysicalInsert::new(
                table_name.clone(),
                columns.clone(),
                input,
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_values_node(columns: &Vec<Column>, tuples: &Vec<Vec<Value>>) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Values(PhysicalValues::new(
                columns.clone(),
                tuples.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_project_node(
        expressions: &Vec<BoundExpression>,
        input: Arc<PhysicalPlanV2>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Project(PhysicalProject::new(
                expressions.clone(),
                input,
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_filter_node(predicate: &BoundExpression, input: Arc<PhysicalPlanV2>) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Filter(PhysicalFilter::new(
                predicate.clone(),
                input,
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_table_scan_node(table_oid: &TableOid, columns: &Vec<Column>) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::TableScan(PhysicalTableScan::new(
                table_oid.clone(),
                columns.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_limit_node(
        limit: &Option<usize>,
        offset: &Option<usize>,
        input: Arc<PhysicalPlanV2>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::Limit(PhysicalLimit::new(
                offset.clone(),
                limit.clone(),
                input,
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_nested_loop_join_node(
        join_type: JoinType,
        condition: Option<BoundExpression>,
        left_input: Arc<PhysicalPlanV2>,
        right_input: Arc<PhysicalPlanV2>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalPlanV2::NestedLoopJoin(PhysicalNestedLoopJoin::new(
                join_type,
                condition,
                left_input,
                right_input,
            ))),
            children: Vec::new(),
        }
    }
}
