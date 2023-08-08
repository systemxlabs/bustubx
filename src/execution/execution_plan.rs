use std::sync::Arc;

use crate::{
    optimizer::operator::PhysicalOperator,
    storage::{table_heap::TableIterator, tuple::Tuple},
};

use super::{
    volcano_executor::{
        create_table::VolcanoCreateTableExecutor, filter::VolcanoFilterExecutor,
        insert::VolcanoInsertExecutor, limit::VolcanLimitExecutor, project::VolcanoProjectExecutor,
        table_scan::VolcanoTableScanExecutor, values::VolcanValuesExecutor, NextResult,
        VolcanoExecutor,
    },
    ExecutionContext,
};

#[derive(Debug)]
pub enum Executor {
    Dummy,
    VolcanoCreateTable(VolcanoCreateTableExecutor),
    VolcanoInsert(VolcanoInsertExecutor),
    VolcanoValues(VolcanValuesExecutor),
    VolcanoTableScan(VolcanoTableScanExecutor),
    VolcanoFilter(VolcanoFilterExecutor),
    VolcanoProject(VolcanoProjectExecutor),
    VolcanoLimit(VolcanLimitExecutor),
}

#[derive(Debug)]
pub struct ExecutionPlan {
    pub executor: Executor,
    pub operator: Arc<PhysicalOperator>,
    pub children: Vec<Arc<ExecutionPlan>>,
}
impl ExecutionPlan {
    pub fn dummy() -> Self {
        Self {
            executor: Executor::Dummy,
            operator: Arc::new(PhysicalOperator::Dummy),
            children: Vec::new(),
        }
    }
    pub fn new_create_table_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoCreateTable(VolcanoCreateTableExecutor {}),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_insert_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoInsert(VolcanoInsertExecutor::new()),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_values_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoValues(VolcanValuesExecutor::new()),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_table_scan_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoTableScan(VolcanoTableScanExecutor::default()),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_filter_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoFilter(VolcanoFilterExecutor {}),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_project_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoProject(VolcanoProjectExecutor {}),
            operator,
            children: Vec::new(),
        }
    }
    pub fn new_limit_node(operator: Arc<PhysicalOperator>) -> Self {
        Self {
            executor: Executor::VolcanoLimit(VolcanLimitExecutor::new()),
            operator,
            children: Vec::new(),
        }
    }
    pub fn init(&self, context: &mut ExecutionContext) {
        match self.executor {
            Executor::Dummy => {}
            Executor::VolcanoCreateTable(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoInsert(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoValues(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoTableScan(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoFilter(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoProject(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoLimit(ref executor) => {
                executor.init(context, self.operator.clone(), self.children.clone())
            }
        }
    }
    pub fn next(&self, context: &mut ExecutionContext) -> NextResult {
        match self.executor {
            Executor::Dummy => NextResult::new(None, true),
            Executor::VolcanoCreateTable(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoInsert(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoValues(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoTableScan(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoFilter(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoProject(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
            Executor::VolcanoLimit(ref executor) => {
                executor.next(context, self.operator.clone(), self.children.clone())
            }
        }
    }
}
