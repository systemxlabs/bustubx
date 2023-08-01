use self::base_table::BoundBaseTableRef;

pub mod base_table;

#[derive(Debug)]
pub enum BoundTableRef {
    Invalid,
    BaseTable(BoundBaseTableRef),
}
