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
impl InsertStatement {
    pub fn bind(
        context: &BinderContext,
        table_name: &ObjectName,
        columns_ident: &Vec<Ident>,
        values: &Values,
    ) -> InsertStatement {
        if let Some(table_info) = context.catalog.get_table_by_name(&table_name.to_string()) {
            let table = BoundBaseTableRef {
                table: table_info.name.clone(),
                oid: table_info.oid,
                alias: None,
                schema: table_info.schema.clone(),
            };
            let mut columns = Vec::new();
            if columns_ident.is_empty() {
                columns = table_info.schema.columns.clone();
            } else {
                for column_ident in columns_ident {
                    if let Some(column) = table_info.schema.get_by_col_name(&column_ident.value) {
                        columns.push(column.clone());
                    } else {
                        panic!(
                            "Column {} not found in table {}",
                            column_ident.value, table_name
                        );
                    }
                }
            }

            let mut records = Vec::new();
            for row in values.rows.iter() {
                let mut record = Vec::new();
                for expr in row {
                    match expr {
                        Expr::Value(value) => {
                            let data_type = columns[record.len()].column_type;
                            record.push(Value::from_sqlparser_value(value, data_type));
                        }
                        _ => unreachable!(),
                    }
                }
                records.push(record);
            }
            return InsertStatement {
                table,
                columns,
                values: records,
            };
        } else {
            panic!("Table {} not found", table_name);
        }
    }
}
