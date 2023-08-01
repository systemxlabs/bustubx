use sqlparser::ast::Statement;

use crate::catalog::{catalog::Catalog, column::Column};

use self::statement::{
    create_table::CreateTableStatement, select::SelectStatement, BoundStatement,
};

pub mod expression;
pub mod statement;
pub mod table_ref;

pub struct BinderContext {
    pub catalog: Catalog,
    pub db_path: String,
}

pub struct Binder {
    // pub context: BinderContext,
}
impl Binder {
    pub fn bind(&mut self, stmt: &Statement) -> BoundStatement {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                BoundStatement::CreateTable(CreateTableStatement::bind(name, columns))
            }
            Statement::Query(query) => BoundStatement::Select(SelectStatement::bind(query)),
            _ => unimplemented!(),
        }
    }
}
