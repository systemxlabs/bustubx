use std::sync::Arc;

use crate::{
    binder::expression::BoundExpression,
    catalog::{
        catalog::TableOid,
        column::Column,
        schema::{self, Schema},
    },
    dbtype::value::Value,
};

use super::operator::{
    create_table::PhysicalCreateTableOperator, filter::PhysicalFilterOperator,
    insert::PhysicalInsertOperator, project::PhysicalProjectOperator,
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
    pub fn new_filter_node(predicate: &BoundExpression) -> Self {
        Self {
            operator: Arc::new(PhysicalOperator::Filter(PhysicalFilterOperator::new(
                predicate.clone(),
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
}
