use std::sync::Arc;

use crate::{
    binder::{statement::BoundStatement, Binder, BinderContext},
    buffer::buffer_pool::BufferPoolManager,
    catalog::{catalog::Catalog, schema::Schema},
    common::config::TABLE_HEAP_BUFFER_POOL_SIZE,
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
            let statement = binder.bind(&stmt);
            println!("{:?}", statement);

            match statement {
                BoundStatement::CreateTable(create_table) => {
                    let schema = Schema::new(create_table.columns);
                    let table_info = self.catalog.create_table(create_table.table_name, schema);
                    println!("{:?}", table_info);
                    continue;
                }
                _ => {}
            }
        }
    }
}
