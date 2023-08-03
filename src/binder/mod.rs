use sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Statement};

use crate::{
    binder::expression::binary_op::{BinaryOperator, BoundBinaryOp},
    catalog::{catalog::Catalog, column::Column},
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

pub mod bind_insert;
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
            // Statement::Query(query) => BoundStatement::Select(self.bind_select()),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => BoundStatement::Insert(self.bind_insert(table_name, columns, source)),
            _ => unimplemented!(),
        }
    }

    pub fn bind_select() -> SelectStatement {
        unimplemented!()
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
            _ => unimplemented!(),
        }
    }
    pub fn bind_table_ref(&self, table_ref: &sqlparser::ast::TableFactor) -> BoundTableRef {
        unimplemented!()
    }
}
