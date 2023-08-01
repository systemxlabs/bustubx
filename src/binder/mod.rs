use sqlparser::ast::{SetExpr, Statement};

use crate::catalog::{catalog::Catalog, column::Column};

use self::statement::{
    create_table::CreateTableStatement, insert::InsertStatement, select::SelectStatement,
    BoundStatement,
};

pub mod expression;
pub mod statement;
pub mod table_ref;

pub struct BinderContext<'a> {
    pub catalog: &'a Catalog,
}

pub struct Binder<'a> {
    pub context: BinderContext<'a>,
}
impl<'a> Binder<'a> {
    pub fn bind(&mut self, stmt: &Statement) -> BoundStatement {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                BoundStatement::CreateTable(CreateTableStatement::bind(name, columns))
            }
            Statement::Query(query) => BoundStatement::Select(SelectStatement::bind(query)),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    BoundStatement::Insert(InsertStatement::bind(
                        &self.context,
                        table_name,
                        columns,
                        values,
                    ))
                } else {
                    unimplemented!()
                }
            }
            _ => unimplemented!(),
        }
    }
}
