use comfy_table::Cell;

use crate::{catalog::Schema, storage::tuple::Tuple};

pub fn print_tuples(tuples: &Vec<Tuple>, schema: &Schema) {
    if tuples.is_empty() {
        return;
    }
    let mut headers = Vec::new();
    for column in &schema.columns {
        headers.push(Cell::new(column.name.clone()));
    }
    let mut table = comfy_table::Table::new();
    table.set_header(headers);

    for tuple in tuples {
        let mut row = Vec::new();
        tuple.all_values(schema).iter().for_each(|v| {
            row.push(Cell::new(format!("{v:?}")));
        });
        table.add_row(row);
    }

    println!("{}", table);
}
