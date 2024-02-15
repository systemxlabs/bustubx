use crate::buffer::BUSTUBX_PAGE_SIZE;
use crate::execution::physical_plan::PhysicalPlan;
use crate::planner::logical_plan::LogicalPlan;
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

pub fn pretty_format_logical_plan(plan: &LogicalPlan) -> String {
    pretty_format_logical_plan_recursively(plan, 0)
}

fn pretty_format_logical_plan_recursively(plan: &LogicalPlan, indent: usize) -> String {
    let mut result = format!("{:indent$}{}", "", plan);

    for input in plan.inputs() {
        result.push('\n');
        result.push_str(&pretty_format_logical_plan_recursively(input, indent + 2));
    }
    result
}

pub fn pretty_format_physical_plan(plan: &PhysicalPlan) -> String {
    pretty_format_physical_plan_recursively(plan, 0)
}

fn pretty_format_physical_plan_recursively(plan: &PhysicalPlan, indent: usize) -> String {
    let mut result = format!("{:indent$}{}", "", plan);

    for input in plan.inputs() {
        result.push('\n');
        result.push_str(&pretty_format_physical_plan_recursively(input, indent + 2));
    }
    result
}

pub fn page_bytes_to_array(bytes: &[u8]) -> [u8; BUSTUBX_PAGE_SIZE] {
    let mut data = [0u8; BUSTUBX_PAGE_SIZE];
    data.copy_from_slice(bytes);
    data
}
