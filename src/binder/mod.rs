use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, Statement, TableFactor, TableWithJoins};

use crate::{
    binder::expression::{
        binary_op::{BinaryOperator, BoundBinaryOp},
        column_ref::BoundColumnRef,
    },
    catalog::{
        catalog::{Catalog, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME},
        column::ColumnFullName,
    },
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
    table_ref::{
        base_table::BoundBaseTableRef,
        join::{BoundJoinRef, JoinType},
        BoundTableRef,
    },
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
                col_name: ColumnFullName::new(None, ident.value.clone()),
            }),
            Expr::CompoundIdentifier(idents) => {
                if idents.len() == 0 {
                    panic!("Invalid column name");
                }
                if idents.len() == 1 {
                    BoundExpression::ColumnRef(BoundColumnRef {
                        col_name: ColumnFullName::new(None, idents[0].value.clone()),
                    })
                } else {
                    BoundExpression::ColumnRef(BoundColumnRef {
                        col_name: ColumnFullName::new(
                            Some(idents[0].value.clone()),
                            idents[1].value.clone(),
                        ),
                    })
                }
            }
            _ => unimplemented!(),
        }
    }
    pub fn bind_from(&self, from: &Vec<TableWithJoins>) -> BoundTableRef {
        let from_tables = from
            .iter()
            .map(|t| self.bind_joins(t))
            .collect::<Vec<BoundTableRef>>();

        // 每个表通过 cross join 连接
        let mut left_table_ref = from_tables[0].clone();
        for right_table_ref in from_tables.iter().skip(1) {
            left_table_ref = BoundTableRef::Join(BoundJoinRef {
                left: Box::new(left_table_ref),
                right: Box::new(right_table_ref.clone()),
                join_type: JoinType::CrossJoin,
                condition: None,
            });
        }
        return left_table_ref;
    }

    pub fn bind_joins(&self, table_with_joins: &TableWithJoins) -> BoundTableRef {
        let mut left_table_ref = self.bind_table_ref(&table_with_joins.relation);
        for join in table_with_joins.joins.iter() {
            let right_table_ref = self.bind_table_ref(&join.relation);
            match join.join_operator {
                JoinOperator::Inner(ref constraint) => {
                    left_table_ref = BoundTableRef::Join(BoundJoinRef {
                        left: Box::new(left_table_ref),
                        right: Box::new(right_table_ref),
                        join_type: JoinType::Inner,
                        condition: Some(self.bind_join_constraint(constraint)),
                    });
                }
                JoinOperator::LeftOuter(ref constraint) => {
                    left_table_ref = BoundTableRef::Join(BoundJoinRef {
                        left: Box::new(left_table_ref),
                        right: Box::new(right_table_ref),
                        join_type: JoinType::LeftOuter,
                        condition: Some(self.bind_join_constraint(constraint)),
                    });
                }
                JoinOperator::RightOuter(ref constraint) => {
                    left_table_ref = BoundTableRef::Join(BoundJoinRef {
                        left: Box::new(left_table_ref),
                        right: Box::new(right_table_ref),
                        join_type: JoinType::RightOuter,
                        condition: Some(self.bind_join_constraint(constraint)),
                    });
                }
                JoinOperator::FullOuter(ref constraint) => {
                    left_table_ref = BoundTableRef::Join(BoundJoinRef {
                        left: Box::new(left_table_ref),
                        right: Box::new(right_table_ref),
                        join_type: JoinType::FullOuter,
                        condition: Some(self.bind_join_constraint(constraint)),
                    });
                }
                JoinOperator::CrossJoin => {
                    left_table_ref = BoundTableRef::Join(BoundJoinRef {
                        left: Box::new(left_table_ref),
                        right: Box::new(right_table_ref),
                        join_type: JoinType::CrossJoin,
                        condition: None,
                    });
                }
                _ => unimplemented!(),
            }
        }
        return left_table_ref;
    }

    fn bind_table_ref(&self, table: &TableFactor) -> BoundTableRef {
        println!("bind_base_table_ref {:?}", table);
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
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let table_ref = self.bind_joins(table_with_joins);
                // TODO 记录alias
                table_ref
            }
            _ => unimplemented!(),
        }
    }

    pub fn bind_join_constraint(&self, constraint: &JoinConstraint) -> BoundExpression {
        match constraint {
            JoinConstraint::On(expr) => self.bind_expression(expr),
            _ => unimplemented!(),
        }
    }
}
