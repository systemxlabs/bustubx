pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub primary: bool,
    pub default: Option<String>,
    pub nullable: bool,
    pub auto_increment: bool,
    pub comment: Option<String>,
}

pub enum DataType {
    Int,
    Varchar(usize),
    Char(usize),
    Date,
    Time,
    Timestamp,
    Float,
    Double,
    Decimal(usize, usize),
}
