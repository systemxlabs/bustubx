use std::sync::Arc;
use tempfile::TempDir;

use tracing::span;

use crate::error::{BustubxError, BustubxResult};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::PhysicalPlanner;
use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    execution::{ExecutionContext, ExecutionEngine},
    planner::{LogicalPlanner, PlannerContext},
    storage::{DiskManager, Tuple},
};

pub struct Database {
    disk_manager: Arc<DiskManager>,
    catalog: Catalog,
    temp_dir: Option<TempDir>,
}
impl Database {
    pub fn new_on_disk(db_path: &str) -> BustubxResult<Self> {
        let disk_manager = Arc::new(DiskManager::try_new(&db_path)?);
        let buffer_pool_manager =
            BufferPoolManager::new(TABLE_HEAP_BUFFER_POOL_SIZE, disk_manager.clone());
        // TODO load catalog from disk
        let catalog = Catalog::new(buffer_pool_manager);
        Ok(Self {
            disk_manager,
            catalog,
            temp_dir: None,
        })
    }

    pub fn new_temp() -> BustubxResult<Self> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager =
            Arc::new(DiskManager::try_new(temp_path.to_str().ok_or(
                BustubxError::Internal("Invalid temp path".to_string()),
            )?)?);
        let buffer_pool_manager =
            BufferPoolManager::new(TABLE_HEAP_BUFFER_POOL_SIZE, disk_manager.clone());
        let catalog = Catalog::new(buffer_pool_manager);
        Ok(Self {
            disk_manager,
            catalog,
            temp_dir: Some(temp_dir),
        })
    }

    pub fn run(&mut self, sql: &str) -> BustubxResult<Vec<Tuple>> {
        let logical_plan = self.build_logical_plan(sql)?;
        println!("{:?}", logical_plan);

        // logical plan -> physical plan
        let physical_plan = PhysicalPlanner::new().create_physical_plan(logical_plan);
        // println!("{:?}", physical_plan);

        let execution_ctx = ExecutionContext::new(&mut self.catalog);
        let mut execution_engine = ExecutionEngine {
            context: execution_ctx,
        };
        let tuples = execution_engine.execute(Arc::new(physical_plan));
        // println!("execution result: {:?}", tuples);
        Ok(tuples)
    }

    pub fn build_logical_plan(&mut self, sql: &str) -> BustubxResult<LogicalPlan> {
        // sql -> ast
        let stmts = crate::parser::parse_sql(sql)?;
        if stmts.len() != 1 {
            return Err(BustubxError::NotSupport(
                "only support one sql statement".to_string(),
            ));
        }
        let stmt = &stmts[0];
        let mut planner = LogicalPlanner {
            context: PlannerContext {
                catalog: &self.catalog,
            },
        };
        // ast -> logical plan
        let logical_plan = planner.plan(&stmt);

        Ok(logical_plan)
    }
}
