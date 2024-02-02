use comfy_table::Cell;

use crate::storage::Tuple;

pub fn pretty_format_tuples(tuples: &Vec<Tuple>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");

    if tuples.is_empty() {
        return table;
    }

    let schema = &tuples[0].schema;

    let mut header = Vec::new();
    for column in schema.columns.iter() {
        header.push(Cell::new(column.name.clone()));
    }
    table.set_header(header);

    for tuple in tuples {
        let mut cells = Vec::new();
        for value in tuple.data.iter() {
            cells.push(Cell::new(format!("{value}")));
        }
        table.add_row(cells);
    }

    table
}
