use self::{create_table::CreateTableStatement, insert::InsertStatement, select::SelectStatement};

pub mod create_table;
pub mod insert;
pub mod select;

#[derive(Debug)]
pub enum BoundStatement {
    CreateTable(CreateTableStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
}
