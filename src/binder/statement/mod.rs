use self::{create_table::CreateTableStatement, select::SelectStatement};

pub mod create_table;
pub mod select;

#[derive(Debug)]
pub enum BoundStatement {
    CreateTable(CreateTableStatement),
    Select(SelectStatement),
}
