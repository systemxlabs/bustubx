use sqlparser::ast::{Expr, Ident, ObjectName, Values};

use crate::{
    binder::{table_ref::base_table::BoundBaseTableRef, BinderContext},
    catalog::column::Column,
    dbtype::value::Value,
};

#[derive(Debug)]
pub struct InsertStatement {
    pub table: BoundBaseTableRef,
    pub columns: Vec<Column>,
    pub values: Vec<Vec<Value>>,
}
