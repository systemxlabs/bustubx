use self::{
    create_index::CreateIndexStatement, create_table::CreateTableStatement,
    insert::InsertStatement, select::SelectStatement,
};

pub mod create_index;
pub mod create_table;
pub mod insert;
pub mod select;

#[derive(Debug)]
pub enum BoundStatement {
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
}
