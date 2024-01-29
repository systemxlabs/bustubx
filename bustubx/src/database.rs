use std::sync::Arc;
use tempfile::TempDir;

use tracing::span;

use crate::error::{BustubxError, BustubxResult};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_planner::PhysicalPlanner;
use crate::{
    buffer::buffer_pool::BufferPoolManager,
    catalog::Catalog,
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
    execution::{ExecutionContext, ExecutionEngine},
    planner::{Planner, PlannerContext},
    storage::{DiskManager, Tuple},
};

pub struct Database {
    disk_manager: Arc<DiskManager>,
    catalog: Catalog,
    temp_dir: Option<TempDir>,
}
impl Database {
    pub fn new_on_disk(db_path: &str) -> Self {
        let disk_manager = Arc::new(DiskManager::try_new(&db_path).unwrap());
        let buffer_pool_manager =
            BufferPoolManager::new(TABLE_HEAP_BUFFER_POOL_SIZE, disk_manager.clone());
        // TODO load catalog from disk
        let catalog = Catalog::new(buffer_pool_manager);
        Self {
            disk_manager,
            catalog,
            temp_dir: None,
        }
    }

    pub fn new_temp() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path.to_str().unwrap()).unwrap());
        let buffer_pool_manager =
            BufferPoolManager::new(TABLE_HEAP_BUFFER_POOL_SIZE, disk_manager.clone());
        let catalog = Catalog::new(buffer_pool_manager);
        Self {
            disk_manager,
            catalog,
            temp_dir: Some(temp_dir),
        }
    }

    pub fn run(&mut self, sql: &str) -> BustubxResult<Vec<Tuple>> {
        let _db_run_span = span!(tracing::Level::INFO, "database.run", sql).entered();

        let logical_plan = self.build_logical_plan(sql)?;
        println!("{:?}", logical_plan);

        // logical plan -> physical plan
        let physical_plan = PhysicalPlanner::new().create_physical_plan(logical_plan);
        // println!("{:?}", physical_plan);

        let execution_ctx = ExecutionContext::new(&mut self.catalog);
        let mut execution_engine = ExecutionEngine {
            context: execution_ctx,
        };
        let (tuples, schema) = execution_engine.execute(Arc::new(physical_plan));
        // println!("execution result: {:?}", tuples);
        // print_tuples(&tuples, &schema);
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
        let mut planner = Planner {
            context: PlannerContext {
                catalog: &self.catalog,
            },
        };
        // ast -> logical plan
        let logical_plan = planner.plan(&stmt);

        Ok(logical_plan)
    }
}

mod tests {
    use crate::{
        catalog::{Column, DataType, Schema},
        common::ScalarValue,
    };

    #[test]
    pub fn test_crud_sql() {
        let mut db = super::Database::new_temp();
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
        let mut db = super::Database::new_temp();
        db.run("create table t1 (a int, b int)");

        let table = db.catalog.get_table_by_name("t1");
        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.name, "t1");
        assert_eq!(table.schema.columns.len(), 2);
        assert_eq!(table.schema.columns[0].name, "a".to_string());
        assert_eq!(table.schema.columns[0].data_type, DataType::Int32);
        assert_eq!(table.schema.columns[1].name, "b".to_string());
        assert_eq!(table.schema.columns[1].data_type, DataType::Int32);
    }

    #[test]
    pub fn test_create_index_sql() {
        let mut db = super::Database::new_temp();
        db.run("create table t1 (a int, b int)");
        db.run("create index idx1 on t1 (a)");

        let index = db.catalog.get_index_by_name("t1", "idx1");
        assert!(index.is_some());
        let index = index.unwrap();
        assert_eq!(index.name, "idx1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.key_schema.column_count(), 1);
    }

    #[test]
    pub fn test_insert_sql() {
        let mut db = super::Database::new_temp();
        db.run(&"create table t1 (a int, b int)".to_string());
        let insert_rows = db
            .run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string())
            .unwrap();
        assert_eq!(insert_rows.len(), 1);

        let schema = Schema::new(vec![Column::new(
            "insert_rows".to_string(),
            DataType::Int32,
        )]);
        let insert_rows = insert_rows[0].get_value_by_col_id(&schema, 0);
        assert_eq!(insert_rows, ScalarValue::Int32(Some(3)));
    }

    #[test]
    pub fn test_select_wildcard_sql() {
        let mut db = super::Database::new_temp();
        db.run(&"create table t1 (a int, b bigint)".to_string());

        let select_result = db.run(&"select * from t1".to_string()).unwrap();
        assert_eq!(select_result.len(), 0);

        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());

        let select_result = db.run(&"select * from t1".to_string()).unwrap();
        assert_eq!(select_result.len(), 3);

        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int32),
            Column::new("b".to_string(), DataType::Int64),
        ]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(1))
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(2))
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            ScalarValue::Int64(Some(3))
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(5))
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            ScalarValue::Int64(Some(4))
        );
    }

    #[test]
    pub fn test_select_where_sql() {
        let mut db = super::Database::new_temp();
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        let select_result = db
            .run(&"select a from t1 where a <= b".to_string())
            .unwrap();
        assert_eq!(select_result.len(), 2);

        let schema = Schema::new(vec![Column::new("a".to_string(), DataType::Int32)]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(1))
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(2))
        );
    }

    #[test]
    pub fn test_select_limit_sql() {
        let mut db = super::Database::new_temp();
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (1, 1), (2, 3), (5, 4)".to_string());
        let select_result = db
            .run(&"select * from t1 limit 1 offset 1".to_string())
            .unwrap();
        assert_eq!(select_result.len(), 1);

        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int32),
            Column::new("b".to_string(), DataType::Int32),
        ]);
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(2))
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            ScalarValue::Int32(Some(3))
        );
    }

    // TODO fix tests
    // #[test]
    // pub fn test_select_cross_join_sql() {
    //     let db_path = "test_select_cross_join_sql.db";
    //     let _ = std::fs::remove_file(db_path);
    //
    //     let mut db = super::Database::new_on_disk(db_path);
    //     db.run(&"create table t1 (a int, b int)".to_string());
    //     db.run(&"create table t2 (a int, b int)".to_string());
    //     db.run(&"insert into t1 values (1, 2), (3, 4)".to_string());
    //     db.run(&"insert into t2 values (5, 6), (7, 8)".to_string());
    //     let select_result = db.run(&"select * from t1, t2".to_string());
    //     assert_eq!(select_result.len(), 4);
    //
    //     let schema = Schema::new(vec![
    //         Column::new("a".to_string(), DataType::Int32, 0),
    //         Column::new("b".to_string(), DataType::Int32, 1),
    //         Column::new("a".to_string(), DataType::Int32, 0),
    //         Column::new("b".to_string(), DataType::Int32, 1),
    //     ]);
    //     // 1st row
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 0),
    //         Value::Integer(1)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 1),
    //         Value::Integer(2)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 2),
    //         Value::Integer(5)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 3),
    //         Value::Integer(6)
    //     );
    //
    //     // 2nd row
    //     assert_eq!(
    //         select_result[1].get_value_by_col_id(&schema, 0),
    //         Value::Integer(1)
    //     );
    //     assert_eq!(
    //         select_result[1].get_value_by_col_id(&schema, 1),
    //         Value::Integer(2)
    //     );
    //     assert_eq!(
    //         select_result[1].get_value_by_col_id(&schema, 2),
    //         Value::Integer(7)
    //     );
    //     assert_eq!(
    //         select_result[1].get_value_by_col_id(&schema, 3),
    //         Value::Integer(8)
    //     );
    //
    //     // 3rd row
    //     assert_eq!(
    //         select_result[2].get_value_by_col_id(&schema, 0),
    //         Value::Integer(3)
    //     );
    //     assert_eq!(
    //         select_result[2].get_value_by_col_id(&schema, 1),
    //         Value::Integer(4)
    //     );
    //     assert_eq!(
    //         select_result[2].get_value_by_col_id(&schema, 2),
    //         Value::Integer(5)
    //     );
    //     assert_eq!(
    //         select_result[2].get_value_by_col_id(&schema, 3),
    //         Value::Integer(6)
    //     );
    //
    //     // 4th row
    //     assert_eq!(
    //         select_result[3].get_value_by_col_id(&schema, 0),
    //         Value::Integer(3)
    //     );
    //     assert_eq!(
    //         select_result[3].get_value_by_col_id(&schema, 1),
    //         Value::Integer(4)
    //     );
    //     assert_eq!(
    //         select_result[3].get_value_by_col_id(&schema, 2),
    //         Value::Integer(7)
    //     );
    //     assert_eq!(
    //         select_result[3].get_value_by_col_id(&schema, 3),
    //         Value::Integer(8)
    //     );
    //
    //     let _ = std::fs::remove_file(db_path);
    // }
    //
    // #[test]
    // pub fn test_select_inner_join_sql() {
    //     let db_path = "test_select_inner_join_sql.db";
    //     let _ = std::fs::remove_file(db_path);
    //
    //     let mut db = super::Database::new_on_disk(db_path);
    //     db.run(&"create table t1 (a int, b int)".to_string());
    //     db.run(&"create table t2 (a int, b int)".to_string());
    //     db.run(&"insert into t1 values (1, 2), (5, 6)".to_string());
    //     db.run(&"insert into t2 values (3, 4), (7, 8)".to_string());
    //     let select_result = db.run(&"select * from t1 inner join t2 on t1.a > t2.a".to_string());
    //     assert_eq!(select_result.len(), 1);
    //
    //     let schema = Schema::new(vec![
    //         Column::new("a".to_string(), DataType::Int32, 0),
    //         Column::new("b".to_string(), DataType::Int32, 0),
    //         Column::new("a".to_string(), DataType::Int32, 0),
    //         Column::new("b".to_string(), DataType::Int32, 0),
    //     ]);
    //     // 1st row
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 0),
    //         Value::Integer(5)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 1),
    //         Value::Integer(6)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 2),
    //         Value::Integer(3)
    //     );
    //     assert_eq!(
    //         select_result[0].get_value_by_col_id(&schema, 3),
    //         Value::Integer(4)
    //     );
    //
    //     let _ = std::fs::remove_file(db_path);
    // }

    #[test]
    pub fn test_select_order_by_sql() {
        let mut db = super::Database::new_temp();
        db.run(&"create table t1 (a int, b int)".to_string());
        db.run(&"insert into t1 values (5, 6), (1, 2), (1, 4)".to_string());
        let select_result = db
            .run(&"select * from t1 order by a, b desc".to_string())
            .unwrap();
        assert_eq!(select_result.len(), 3);

        let schema = Schema::new(vec![
            Column::new("a".to_string(), DataType::Int32),
            Column::new("b".to_string(), DataType::Int32),
        ]);

        // 1st row
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(1))
        );
        assert_eq!(
            select_result[0].get_value_by_col_id(&schema, 1),
            ScalarValue::Int32(Some(4))
        );

        // 2nd row
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(1))
        );
        assert_eq!(
            select_result[1].get_value_by_col_id(&schema, 1),
            ScalarValue::Int32(Some(2))
        );

        // 3th row
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 0),
            ScalarValue::Int32(Some(5))
        );
        assert_eq!(
            select_result[2].get_value_by_col_id(&schema, 1),
            ScalarValue::Int32(Some(6))
        );
    }
}
