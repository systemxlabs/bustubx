use sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Statement, TableFactor};

use crate::{
    binder::expression::{
        binary_op::{BinaryOperator, BoundBinaryOp},
        column_ref::BoundColumnRef,
    },
    catalog::{
        catalog::{Catalog, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME},
        column::Column,
    },
    dbtype::value::Value,
};

use self::{
    expression::{
        constant::{BoundConstant, Constant},
        BoundExpression,
    },
    statement::{
        create_table::CreateTableStatement, insert::InsertStatement, select::SelectStatement,
        BoundStatement,
    },
    table_ref::{base_table::BoundBaseTableRef, BoundTableRef},
};

pub mod bind_create_table;
pub mod bind_insert;
pub mod bind_select;
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
                BoundStatement::CreateTable(self.bind_create_table(name, columns))
            }
            Statement::Query(query) => BoundStatement::Select(self.bind_select(query)),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => BoundStatement::Insert(self.bind_insert(table_name, columns, source)),
            _ => unimplemented!(),
        }
    }

    pub fn bind_expression(&self, expr: &Expr) -> BoundExpression {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let op = BinaryOperator::from_sqlparser_operator(op);
                let larg = Box::new(self.bind_expression(left));
                let rarg = Box::new(self.bind_expression(right));
                BoundExpression::BinaryOp(BoundBinaryOp { larg, op, rarg })
            }
            Expr::Value(value) => BoundExpression::Constant(BoundConstant {
                value: Constant::from_sqlparser_value(value),
            }),
            Expr::Identifier(ident) => BoundExpression::ColumnRef(BoundColumnRef {
                col_names: vec![ident.value.clone()],
            }),
            _ => unimplemented!(),
        }
    }
    pub fn bind_table_ref(&self, table: &TableFactor) -> BoundTableRef {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (_database, _schema, table) = match name.0.as_slice() {
                    [table] => (
                        DEFAULT_DATABASE_NAME,
                        DEFAULT_SCHEMA_NAME,
                        table.value.as_str(),
                    ),
                    [schema, table] => (
                        DEFAULT_DATABASE_NAME,
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    [db, schema, table] => (
                        db.value.as_str(),
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    _ => unimplemented!(),
                };

                let table_info = self.context.catalog.get_table_by_name(table);
                if table_info.is_none() {
                    panic!("Table {} not found", table);
                }
                let table_info = table_info.unwrap();

                let alias = alias.as_ref().map(|a| a.name.value.clone());

                BoundTableRef::BaseTable(BoundBaseTableRef {
                    table: table.to_string(),
                    oid: table_info.oid,
                    alias,
                    schema: table_info.schema.clone(),
                })
            }
            _ => unimplemented!(),
        }
    }
}
