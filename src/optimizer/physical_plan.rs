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
    create_table::PhysicalCreateTableOperator, filter::PhysicalFilterOperator,
    insert::PhysicalInsertOperator, limit::PhysicalLimitOperator,
    nested_loop_join::PhysicalNestedLoopJoinOperator, project::PhysicalProjectOperator,
    table_scan::PhysicalTableScanOperator, values::PhysicalValuesOperator, PhysicalOperator,
};

#[derive(Debug)]
pub struct PhysicalPlan {
    pub operator: Arc<PhysicalOperator>,
    pub children: Vec<Arc<PhysicalPlan>>,
}
impl PhysicalPlan {
    pub fn output_schema(&self) -> Schema {
        self.operator.output_schema()
    }
    pub fn dummy() -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Dummy),
            children: Vec::new(),
        }
    }
    pub fn new_create_table_node(table_name: &String, schema: &Schema) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::CreateTable(
                PhysicalCreateTableOperator::new(table_name.clone(), schema.clone()),
            )),
            children: Vec::new(),
        }
    }
    pub fn new_insert_node(table_name: &String, columns: &Vec<Column>) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Insert(PhysicalInsertOperator::new(
                table_name.clone(),
                columns.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_values_node(columns: &Vec<Column>, tuples: &Vec<Vec<Value>>) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Values(PhysicalValuesOperator::new(
                columns.clone(),
                tuples.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_project_node(expressions: &Vec<BoundExpression>) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Project(PhysicalProjectOperator::new(
                expressions.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_filter_node(predicate: &BoundExpression, input: Arc<PhysicalOperator>) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Filter(PhysicalFilterOperator::new(
                predicate.clone(),
                input,
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_table_scan_node(table_oid: &TableOid, columns: &Vec<Column>) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::TableScan(PhysicalTableScanOperator::new(
                table_oid.clone(),
                columns.clone(),
            ))),
            children: Vec::new(),
        }
    }
    pub fn new_limit_node(
        limit: &Option<usize>,
        offset: &Option<usize>,
        input: Arc<PhysicalOperator>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Limit(PhysicalLimitOperator::new(
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
        left_input: Arc<PhysicalOperator>,
        right_input: Arc<PhysicalOperator>,
    ) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::NestedLoopJoin(
                PhysicalNestedLoopJoinOperator::new(join_type, condition, left_input, right_input),
            )),
            children: Vec::new(),
        }
    }
}
