use std::sync::Arc;

use crate::{
    binder::{Binder, BinderContext},
    buffer::buffer_pool::BufferPoolManager,
    catalog::catalog::Catalog,
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    execution::{ExecutionContext, ExecutionEngine},
    optimizer::Optimizer,
    planner::Planner,
    storage::{disk_manager::DiskManager, tuple::Tuple},
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

    pub fn run(&mut self, sql: &String) -> Vec<Tuple> {
        // sql -> ast
        let stmts = crate::parser::parse_sql(sql);
        if stmts.is_err() {
            println!("parse sql error");
            return Vec::new();
        }
        let stmts = stmts.unwrap();
        if stmts.len() != 1 {
            println!("only support one sql statement");
            return Vec::new();
        }
        let stmt = &stmts[0];
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
        let result = execution_engine.execute(execution_plan);
        println!("{:?}", result);
        result
    }
}

mod tests {
    use crate::{
        catalog::{
            column::{Column, ColumnFullName, DataType},
            schema::Schema,
        },
        dbtype::value::Value,
    };

    #[test]
    pub fn test_crud_sql() {
        let mut db = super::Database::new_on_disk("test.db");
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"create table t2 (a int, b int)".to_string());
        db.run(&"create table t3 (a int, b int)".to_string());
        // db.run(&"create table t4 (a int, b int)".to_string());
        // db.run(&"select * from t1, t2, t3 inner join t4 on t3.id = t4.id".to_string());
        // db.run(&"select * from (t1 inner join t2 on t1.a = t2.a) inner join t3 on t1.a = t3.a ".to_string());
    }

    #[test]
    pub fn test_create_table_sql() {
        let db_path = "test_create_table_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());

        let table = db.catalog.get_table_by_name("t1");
        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.name, "t1");
        assert_eq!(table.schema.columns.len(), 2);
        assert_eq!(
            table.schema.columns[0].full_name,
            ColumnFullName::new(Some("t1".to_string()), "a".to_string())
        );
        assert_eq!(table.schema.columns[0].column_type, DataType::Integer);
        assert_eq!(
            table.schema.columns[1].full_name,
            ColumnFullName::new(Some("t1".to_string()), "b".to_string())
        );
        assert_eq!(table.schema.columns[1].column_type, DataType::Integer);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_insert_sql() {
        let db_path = "test_insert_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        let insert_rows = db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        assert_eq!(insert_rows.len(), 1);

        let schema = Schema::new(vec![Column::new(
            None,
            "insert_rows".to_string(),
            DataType::Integer,
            0,
        )]);
        let insert_rows = insert_rows[0].get_value_by_col_id(&schema, 0);
        assert_eq!(insert_rows, Value::Integer(3));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_select_wildcard_sql() {
        let db_path = "test_select_wildcard_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());

        let select_result = db.run(&"select * from t1".to_string());
        assert_eq!(select_result.len(), 0);

        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());

        let select_result = db.run(&"select * from t1".to_string());
        assert_eq!(select_result.len(), 3);

        let schema = Schema::new(vec![
            Column::new(
                Some("t1".to_string()),
                "a".to_string(),
                DataType::Integer,
                0,
            ),
            Column::new(
                Some("t1".to_string()),
                "b".to_string(),
                DataType::Integer,
                1,
            ),
        ]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            Value::Integer(2)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            Value::Integer(3)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            Value::Integer(4)
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_select_where_sql() {
        let db_path = "test_select_where_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        let select_result = db.run(&"select a from t1 where a <= b".to_string());
        assert_eq!(select_result.len(), 2);

        let schema = Schema::new(vec![Column::new(
            Some("t1".to_string()),
            "a".to_string(),
            DataType::Integer,
            0,
        )]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            Value::Integer(2)
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_select_limit_sql() {
        let db_path = "test_select_limit_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        let select_result = db.run(&"select * from t1 limit 1 offset 1".to_string());
        assert_eq!(select_result.len(), 1);

        let schema = Schema::new(vec![
            Column::new(
                Some("t1".to_string()),
                "a".to_string(),
                DataType::Integer,
                0,
            ),
            Column::new(
                Some("t1".to_string()),
                "b".to_string(),
                DataType::Integer,
                1,
            ),
        ]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(2)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::Integer(3)
        );

        let _ = std::fs::remove_file(db_path);
    }
}
