use sqlparser::ast::{Ident, ObjectName, Query, SetExpr};

use super::{
    expression::BoundExpression, statement::insert::InsertStatement,
    table_ref::base_table::BoundBaseTableRef, Binder,
};

impl<'a> Binder<'a> {
    pub fn bind_insert(
        &self,
        table_name: &ObjectName,
        columns_ident: &Vec<Ident>,
        source: &Query,
    ) -> InsertStatement {
        if let SetExpr::Values(values) = source.body.as_ref() {
            if let Some(table_info) = self
                .context
                .catalog
                .get_table_by_name(&table_name.to_string())
            {
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
                        if let Some(column) = table_info.schema.get_by_col_name(&column_ident.value)
                        {
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
                        let data_type = columns[record.len()].column_type;
                        if let BoundExpression::Constant(constant) = self.bind_expression(expr) {
                            record.push(constant.value.to_value(data_type));
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
        } else {
            unimplemented!()
        }
    }
}
