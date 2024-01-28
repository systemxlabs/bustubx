use sqlparser::{
    ast::Statement,
    dialect::PostgreSqlDialect,
    parser::{Parser, ParserError},
};
use tracing::span;

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let _parse_sql_span = span!(tracing::Level::INFO, "parse_sql", sql).entered();
    Parser::parse_sql(&PostgreSqlDialect {}, sql)
}

mod tests {
    #[test]
    pub fn test_sql() {
        let sql = "select * from t1, t2, t3 inner join t4 on t3.id = t4.id";
        let stmts = super::parse_sql(sql);
        println!("{:?}", stmts);
    }
}
