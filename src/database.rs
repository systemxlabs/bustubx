use std::sync::Arc;

use crate::{
    binder::{statement::BoundStatement, Binder, BinderContext},
    buffer::buffer_pool::BufferPoolManager,
    catalog::{catalog::Catalog, schema::Schema},
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    execution::{ExecutionContext, ExecutionEngine},
    optimizer::Optimizer,
    planner::Planner,
    storage::disk_manager::DiskManager,
};

pub struct Database {
    disk_manager: Arc<DiskManager>,
    catalog: Catalog,
}
impl Database {
    pub fn new_on_disk(db_path: &str) -> Self {
        let disk_manager = Arc::new(DiskManager::new(db_path.to_string()));
        let buffer_pool_manager =
            BufferPoolManager::new(TABLE_HEAP_BUFFER_POOL_SIZE, disk_manager.clone());
        let catalog = Catalog::new(buffer_pool_manager);
        Self {
            disk_manager,
            catalog,
        }
    }

    pub fn run(&mut self, sql: &String) {
        // sql -> ast
        let stmts = crate::parser::parse_sql(sql);
        if stmts.is_err() {
            println!("parse sql error");
            return;
        }
        let stmts = stmts.unwrap();
        for stmt in stmts {
            let mut binder = Binder {
                context: BinderContext {
                    catalog: &self.catalog,
                },
            };
            // ast -> statement
            let statement = binder.bind(&stmt);
            // println!("{:?}", statement);

            // statement -> logical plan
            let mut planner = Planner {};
            let logical_plan = planner.plan(statement);
            // println!("{:?}", logical_plan);

            // logical plan -> physical plan
            let mut optimizer = Optimizer::new(Arc::new(logical_plan));
            let physical_plan = optimizer.find_best();
            // println!("{:?}", physical_plan);

            let execution_ctx = ExecutionContext::new(&mut self.catalog);
            let mut execution_engine = ExecutionEngine {
                context: execution_ctx,
            };
            let execution_plan = execution_engine.plan(Arc::new(physical_plan));
            // println!("{:?}", execution_plan);
            execution_engine.execute(execution_plan);
        }
    }
}

mod tests {
    #[test]
    pub fn test_crud_sql() {
        let mut db = super::Database::new_on_disk("test.db");
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        db.run(&"select * from t1".to_string());
        db.run(&"select a from t1 where a <= b".to_string());
        db.run(&"select * from t1 limit 1 offset 1".to_string());
    }
}
