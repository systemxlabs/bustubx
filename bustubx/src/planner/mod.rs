use sqlparser::ast::{JoinConstraint, JoinOperator, Statement, TableFactor, TableWithJoins};
use std::sync::Arc;

use crate::common::table_ref::TableReference;
use crate::{
    catalog::catalog::{Catalog, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME},
    planner::expr::{
        binary_op::{BinaryOp, BinaryOperator},
        column_ref::ColumnRef,
    },
};
use logical_plan::LogicalPlan;
use operator::LogicalOperator;

use self::{
    expr::{
        constant::{BoundConstant, Constant},
        Expr,
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
pub mod expr;
pub mod logical_plan;
pub mod logical_plan_v2;
pub mod operator;
pub mod order_by;
pub mod physical_plan;
pub mod physical_planner;
pub mod plan_create_index;
pub mod table_ref;

pub struct PlannerContext<'a> {
    pub catalog: &'a Catalog,
}

pub struct Planner<'a> {
    pub context: PlannerContext<'a>,
}
impl<'a> Planner<'a> {
    pub fn plan(&mut self, stmt: &Statement) -> LogicalPlan {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.plan_create_table(name, columns),
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                ..
            } => self.plan_create_index(name, table_name, columns),
            Statement::Query(query) => self.plan_select(query),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.plan_insert(table_name, columns, source),
            _ => unimplemented!(),
        }
    }

    pub fn bind_expression(&self, expr: &sqlparser::ast::Expr) -> Expr {
        match expr {
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let op = BinaryOperator::from_sqlparser_operator(op);
                let larg = Box::new(self.bind_expression(left));
                let rarg = Box::new(self.bind_expression(right));
                Expr::BinaryOp(BinaryOp { larg, op, rarg })
            }
            sqlparser::ast::Expr::Value(value) => Expr::Constant(BoundConstant {
                value: Constant::from_sqlparser_value(value),
            }),
            sqlparser::ast::Expr::Identifier(_) | sqlparser::ast::Expr::CompoundIdentifier(_) => {
                Expr::ColumnRef(self.bind_column_ref_expr(expr))
            }
            _ => unimplemented!(),
        }
    }

    pub fn bind_column_ref_expr(&self, expr: &sqlparser::ast::Expr) -> ColumnRef {
        match expr {
            sqlparser::ast::Expr::Identifier(ident) => ColumnRef {
                relation: None,
                col_name: ident.value.clone(),
            },
            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                if idents.len() == 0 {
                    panic!("Invalid column name");
                }
                if idents.len() == 1 {
                    ColumnRef {
                        relation: None,
                        col_name: idents[0].value.clone(),
                    }
                } else {
                    ColumnRef {
                        relation: Some(TableReference::Bare {
                            table: idents[0].value.clone(),
                        }),
                        col_name: idents[1].value.clone(),
                    }
                }
            }
            _ => unreachable!(),
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
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (_catalog, _schema, table) = match name.0.as_slice() {
                    [table] => (
                        DEFAULT_CATALOG_NAME,
                        DEFAULT_SCHEMA_NAME,
                        table.value.as_str(),
                    ),
                    [schema, table] => (
                        DEFAULT_CATALOG_NAME,
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    [catalog, schema, table] => (
                        catalog.value.as_str(),
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    _ => unimplemented!(),
                };

                let alias = alias.as_ref().map(|a| a.name.value.clone());
                BoundTableRef::BaseTable(self.bind_base_table_by_name(table, alias))
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

    pub fn bind_base_table_by_name(
        &self,
        table_name: &str,
        alias: Option<String>,
    ) -> BoundBaseTableRef {
        let table_info = self.context.catalog.get_table_by_name(table_name);
        if table_info.is_none() {
            panic!("Table {} not found", table_name);
        }
        let table_info = table_info.unwrap();

        BoundBaseTableRef {
            table: table_name.to_string(),
            oid: table_info.oid,
            alias,
            schema: table_info.schema.clone(),
        }
    }

    pub fn bind_join_constraint(&self, constraint: &JoinConstraint) -> Expr {
        match constraint {
            JoinConstraint::On(expr) => self.bind_expression(expr),
            _ => unimplemented!(),
        }
    }

    fn plan_table_ref(&mut self, table_ref: BoundTableRef) -> LogicalPlan {
        match table_ref {
            BoundTableRef::BaseTable(table) => LogicalPlan {
                operator: LogicalOperator::new_scan_operator(table.oid, table.schema.columns),
                children: Vec::new(),
            },
            BoundTableRef::Join(join) => {
                let left_plan = self.plan_table_ref(*join.left);
                let right_plan = self.plan_table_ref(*join.right);
                let join_plan = LogicalPlan {
                    operator: LogicalOperator::new_join_operator(join.join_type, join.condition),
                    children: vec![Arc::new(left_plan), Arc::new(right_plan)],
                };
                join_plan
            }
            _ => unimplemented!(),
        }
    }
}
