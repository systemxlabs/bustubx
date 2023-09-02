use std::{sync::Arc, thread::sleep, time::Duration};

use tracing::span;

use crate::{
    binder::{Binder, BinderContext},
    buffer::buffer_pool::BufferPoolManager,
    catalog::catalog::Catalog,
    common::{config::TABLE_HEAP_BUFFER_POOL_SIZE, util::print_tuples},
    execution::{ExecutionContext, ExecutionEngine},
    optimizer::Optimizer,
    planner::{logical_plan::LogicalPlan, Planner},
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
        // TODO load catalog from disk
        let catalog = Catalog::new(buffer_pool_manager);
        Self {
            disk_manager,
            catalog,
        }
    }

    pub fn run(&mut self, sql: &str) -> Vec<Tuple> {
        let _db_run_span = span!(tracing::Level::INFO, "database.run", sql).entered();
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
        println!("{:?}", statement);

        // statement -> logical plan
        let mut planner = Planner {};
        let logical_plan = planner.plan(statement);
        // println!("{:#?}", logical_plan);

        // logical plan -> physical plan
        let mut optimizer = Optimizer::new(logical_plan);
        let physical_plan = optimizer.find_best();
        // println!("{:?}", physical_plan);

        let execution_ctx = ExecutionContext::new(&mut self.catalog);
        let mut execution_engine = ExecutionEngine {
            context: execution_ctx,
        };
        let (tuples, schema) = execution_engine.execute(Arc::new(physical_plan));
        // println!("execution result: {:?}", tuples);
        // print_tuples(&tuples, &schema);
        tuples
    }

    pub fn build_logical_plan(&mut self, sql: &str) -> LogicalPlan {
        // sql -> ast
        let stmts = crate::parser::parse_sql(sql);
        if stmts.is_err() {
            panic!("parse sql error")
        }
        let stmts = stmts.unwrap();
        if stmts.len() != 1 {
            panic!("only support one sql statement")
        }
        let stmt = &stmts[0];
        let mut binder = Binder {
            context: BinderContext {
                catalog: &self.catalog,
            },
        };
        // ast -> statement
        let statement = binder.bind(&stmt);

        // statement -> logical plan
        let mut planner = Planner {};
        planner.plan(statement)
    }
}

mod tests {
    use crate::{
        catalog::{
            column::{Column, ColumnFullName},
            schema::Schema,
        },
        dbtype::{data_type::DataType, value::Value},
    };

    #[test]
    pub fn test_crud_sql() {
        let mut db = super::Database::new_on_disk("test.db");
        // db.run("create table t1 (a int, b int)");
        // db.run("create table t2 (a int, b int)");
        // db.run("create table t3 (a int, b int)");
        // db.run("create table t4 (a int, b int)");
        // db.run("create index idx1 on t1 (a)");
        // db.run("select * from t1 where a > 3");
        // db.run("select * from t1, t2, t3 inner join t4 on t3.id = t4.id");
        // db.run(&"select * from (t1 inner join t2 on t1.a = t2.a) inner join t3 on t1.a = t3.a ".to_string());
    }

    #[test]
    pub fn test_create_table_sql() {
        let db_path = "test_create_table_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run("create table t1 (a int, b int)");

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
    pub fn test_create_index_sql() {
        let db_path = "test_create_index_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run("create table t1 (a int, b int)");
        db.run("create index idx1 on t1 (a)");

        let index = db.catalog.get_index_by_name("t1", "idx1");
        assert!(index.is_some());
        let index = index.unwrap();
        assert_eq!(index.name, "idx1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.key_schema.column_count(), 1);

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
        db.run(&"create table t1 (a int, b bigint)".to_string());

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
            Column::new(Some("t1".to_string()), "b".to_string(), DataType::BigInt, 1),
        ]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::BigInt(1)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            Value::Integer(2)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            Value::BigInt(3)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            Value::BigInt(4)
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

    #[test]
    pub fn test_select_cross_join_sql() {
        let db_path = "test_select_cross_join_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"create table t2 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 2), (3, 4)".to_string());
        db.run(&"insert into t2 values (5, 6), (7, 8)".to_string());
        let select_result = db.run(&"select * from t1, t2".to_string());
        assert_eq!(select_result.len(), 4);

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
            Column::new(
                Some("t2".to_string()),
                "a".to_string(),
                DataType::Integer,
                0,
            ),
            Column::new(
                Some("t2".to_string()),
                "b".to_string(),
                DataType::Integer,
                1,
            ),
        ]);
        // 1st row
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::Integer(2)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 2),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 3),
            Value::Integer(6)
        );

        // 2nd row
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            Value::Integer(2)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 2),
            Value::Integer(7)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 3),
            Value::Integer(8)
        );

        // 3rd row
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            Value::Integer(3)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            Value::Integer(4)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 2),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 3),
            Value::Integer(6)
        );

        // 4th row
        assert_eq!(
            select_result[3].get_value_by_col_id(&schema, 0),
            Value::Integer(3)
        );
        assert_eq!(
            select_result[3].get_value_by_col_id(&schema, 1),
            Value::Integer(4)
        );
        assert_eq!(
            select_result[3].get_value_by_col_id(&schema, 2),
            Value::Integer(7)
        );
        assert_eq!(
            select_result[3].get_value_by_col_id(&schema, 3),
            Value::Integer(8)
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_select_inner_join_sql() {
        let db_path = "test_select_inner_join_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"create table t2 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 2), (5, 6)".to_string());
        db.run(&"insert into t2 values (3, 4), (7, 8)".to_string());
        let select_result = db.run(&"select * from t1 inner join t2 on t1.a > t2.a".to_string());
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
                0,
            ),
            Column::new(
                Some("t2".to_string()),
                "a".to_string(),
                DataType::Integer,
                0,
            ),
            Column::new(
                Some("t2".to_string()),
                "b".to_string(),
                DataType::Integer,
                0,
            ),
        ]);
        // 1st row
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::Integer(6)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 2),
            Value::Integer(3)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 3),
            Value::Integer(4)
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    pub fn test_select_order_by_sql() {
        let db_path = "test_select_order_by_sql.db";
        let _ = std::fs::remove_file(db_path);

        let mut db = super::Database::new_on_disk(db_path);
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (5, 6), (1, 2), (1, 4)".to_string());
        let select_result = db.run(&"select * from t1 order by a, b desc".to_string());
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
                0,
            ),
        ]);

        // 1st row
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            Value::Integer(4)
        );

        // 2nd row
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            Value::Integer(1)
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            Value::Integer(2)
        );

        // 3th row
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            Value::Integer(5)
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            Value::Integer(6)
        );

        let _ = std::fs::remove_file(db_path);
    }
}
