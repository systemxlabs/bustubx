use crate::error::BustubxResult;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use tracing::span;

pub fn parse_sql(sql: &str) -> BustubxResult<Vec<Statement>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    Ok(stmts)
}

mod tests {
    #[test]
    pub fn test_sql() {
        let sql = "select * from t1, t2, t3 inner join t4 on t3.id = t4.id";
        let stmts = super::parse_sql(sql).unwrap();
        println!("{:?}", stmts);
    }
}
